from typing import Dict

from boto3 import client
import fitz
import os
import json
import itertools

from core.database.db import Session as db
from helpers import (
    cleanup_tokens,
    extract_tags_from_leftovers, get_tokens, mark_pid_links, mark_tokens_in_equipment_list,
    get_tokens_matching_part_of_equipment_list_item, group_mapped_tokens, group_unmapped_tokens,
)

from data.job import job as data_job
from data.pid_file import pid_file as data_pid_file
from data.pid_tag import pid_tag as data_pid_tag
from data.pid_file_page import pid_file_page as data_pid_file_page
from data.pid_file_link import pid_file_link as data_pid_file_link
from data.equipment_list import equipment_list as data_equipment_list
from data.equipment_list_item import equipment_list_item as data_equipment_list_item


BUCKET_NAME = os.getenv("BUCKET_NAME","643553455790-eu-west-1-files")

s3 = client("s3")


def process_document(doc, equipment_list_tags):

    pages = []
    for page_number in range(len(doc)):
        validated_tags = []
        page = doc[page_number]

        rotation = page.rotation
        page_width = page.rect.width
        page_height = page.rect.height

        # Step 1: Extract all text from fields and create tokens from it
        tokens = get_tokens(page)
        raw_tokens = tokens

        # Step 2: Split tokens from PID Links
        tokens, pid_links = mark_pid_links(tokens)

        # Step 3: Split tokens from validated tags (based on equipment list)
        # If a token is 100% matching a tag from the equipment list, there is no doubt about it beining not a tag
        tokens, tags = mark_tokens_in_equipment_list(tokens,equipment_list_tags)
        validated_tags.extend(tags)


        # Step 4: Cleanup tokens (eliminate tokens that almost certaintly not a tag)
        tokens, discarded_tokens = cleanup_tokens(tokens)

        # Step 5: Check if tokens are matching part of a tag (like (LS3 in LS3137))
        tokens, mapped_token_dict = get_tokens_matching_part_of_equipment_list_item(tokens, equipment_list_tags, validated_tags)

        # Step 6: Group mapped tokens
        grouped_mapped_tags, leftover_tokens = group_mapped_tokens(mapped_token_dict)
        validated_tags.extend(grouped_mapped_tags)
        tokens.extend(leftover_tokens)

        # Step 7: Group unmapped tokens
        grouped_unmapped_tags, tokens = group_unmapped_tokens(tokens)
        print(f"length: {len(grouped_unmapped_tags)}")
        validated_tags.extend(grouped_unmapped_tags)

        # Step 8: Extract tokens based on Regexes etc
        regex_tags, tokens = extract_tags_from_leftovers(tokens)
        validated_tags.extend(regex_tags)


        page_meta = {
            "page_number": page_number + 1,
            "rotation": rotation,
            "width": page_width,
            "height": page_height,
            "raw_tokens": [t.to_dict() for t in raw_tokens],
            "validated_tags": [t.to_dict() for t in validated_tags],
            "discarded_tokens": [t.to_dict() for t in discarded_tokens],
            "pid_links": [p.to_dict() for p in pid_links],
            "leftovers" : [t.to_dict() for t in tokens]
        }
        pages.append(page_meta)


    return pages


def get_file_from_s3(key):
    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    file_bytes = response["Body"].read()
    return file_bytes

def persist_page_info(page, file_id):

    pid_file_page = data_pid_file_page(
        pid_file_id=file_id,
        page_number=page.get("page_number"),
        height=page.get("height"),
        width=page.get("width"),
        rotation=page.get("rotation"),
        image_s3_key=""  # TODO: save PNG image to S3
    )
    pid_file_page.save()

    return pid_file_page.id

def persist_tags(tags, tag_type, page_id, session):
    visited = {}
    pid_tags = []
    for tag in tags:
        print(tag)
        text = tag.get("text")
        if text in visited.keys():
            value = visited[text] + 1
        else:
            value = 1
        visited[text] = value

        tag_name = f"{text}_{str(value)}"
        pid_tag = data_pid_tag(
            pid_file_page_id=page_id,
            tag_value=text,
            name=tag_name,
            type=tag_type,
            sub_type= tag.get("token_type"),
            x0=tag.get("x0"),
            x1=tag.get("x1"),
            y0=tag.get("y0"),
            y1=tag.get("y1"),
            confidence=1.0,
            candidates = tag.get("candidates")
        )
        pid_tags.append(pid_tag)
    data_pid_tag.bulk_upsert(pid_tags, session)

def persist_pid_links(links, page_id, file_id):
    if len(links) == 0:
        return
    document_identifier = max(links, key=lambda l: l.x0 * l.y0)
    pid_file = data_pid_file.from_id(file_id)
    pid_file.technical_name = document_identifier.text
    pid_file.save()


    for l in links:
        if l.id == document_identifier.id:
            continue

        pid_file_link = data_pid_file_link(
            pid_file_page_id=page_id,
            pid_file_id=file_id,
            type ="RAW",
            name = l.text,
            x1= l.x0,
            x2= l.x1,
            y1=l.y0,
            y2=l.y1,
            image_s3_key=""
        )
        pid_file_link.save()


    return

def persist_results(results, file_id):
    with db() as session:
        for page in results:
            page_id = persist_page_info(page, file_id)

            print("Persist raw tags")
            raw_tags = page.get("raw_tokens",[])
            persist_tags(raw_tags,"RAW",page_id, session)

            print("Persist validated tags")
            extracted_tags = page.get("validated_tags", [])
            persist_tags(extracted_tags, "VALIDATED", page_id, session)

            print("Persist leftovers tags")

            leftover_tags = page.get("leftovers", [])
            persist_tags(leftover_tags, "LEFTOVERS", page_id, session)

            print("Persist discarded tags")
            discarded_tokens = page.get("discarded_tokens", [])
            persist_tags(discarded_tokens, "DISCARDED_TOKENS", page_id, session)

            print("Persist pid links")
            pid_links = page.get("pid_links",[])
            persist_pid_links(pid_links,page_id,file_id)

        session.commit()

        session.close()

def get_tags_from_equipment_list(equipment_list_items):
    tags = []
    for item in equipment_list_items:
        if item.field == "TAG":
            tags.append(str(item.value).upper())
    return tags

def process_record(record):
    print("Processing record:", record)
    body = record.get("body")
    data = json.loads(body)

    job_id = data.get("job_id")
    disable_persist = data.get("disable_persist", None)
    job = data_job.from_id(job_id)
    job.status = "PROCESSING"
    job.save()
    print(f"Processing job ID: {job_id},{job.to_dict()}")

    file_id = data.get("file_id")
    pid_file = data_pid_file.from_id(file_id)
    print(f"Processing file ID: {file_id},{pid_file.to_dict()}")

    equipment_list = data_equipment_list.get(project_id=pid_file.project_id)
    if equipment_list:
        equip_items = data_equipment_list_item.get_all(equipment_list_id= equipment_list.id)
    else:
        equip_items = []
    equipment_list_tags = get_tags_from_equipment_list(equip_items)
    print("EQUIPMENT_LIST_TAGS: ",equipment_list_tags)

    key = pid_file.s3_key
    file_bytes = get_file_from_s3(key)
    doc = fitz.open(stream=file_bytes, filetype="pdf")
    processed_document = process_document(doc, equipment_list_tags)
    try:

        pass

    except Exception as e:
        job.status = "FAILED"
        job.error_message = str(e)
        job.save()
        print(f"Error processing PDF: {e}")
        return

    if not disable_persist:
        persist_results(processed_document,file_id)
    job.status = "COMPLETED"
    job.save()

    return job.to_dict()

def handler(event, context):
    for record in event.get("Records", []):
        process_record(record)

event = {
    "Records" : [
        {
            "body" : json.dumps(
                {
                    "job_id":1,
                    "file_id":1,
                    #"disable_persist": True
                }
            )
        }
    ]
}
handler(event,None)

