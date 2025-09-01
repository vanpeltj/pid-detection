import itertools
import re
import uuid
import yaml
from dataclasses import dataclass, replace, field
from typing import Text, List, Dict, Union, Tuple
from data.pid_tag import pid_tag as data_pid_tag


with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

@dataclass
class Token:
    text: str
    x0: float; y0: float; x1: float; y1: float
    page: int = 0
    page_width: int = 0
    page_height: int = 0

    token_type: str = "RAW"

    candidates: Dict = field(default_factory=dict)

    def __post_init__(self):
        self.id = str(uuid.uuid4())

    def set_token_type(self, token_type):
        self.token_type = token_type

    @property
    def cx(self): return (self.x0 + self.x1) / 2
    @property
    def cy(self): return (self.y0 + self.y1) / 2
    @property
    def h(self): return abs(self.y1 - self.y0)
    @property
    def w(self): return abs(self.x1 - self.x0)

    def to_dict(self):
        return {
            "id": self.id,
            "text": self.text,
            "x0": self.x0,
            "y0": self.y0,
            "x1": self.x1,
            "y1": self.y1,
            "cx": self.cx,
            "cy": self.cy,
            "h": self.h,
            "w": self.w,
            "page_width": self.page_width,
            "page_height": self.page_height,
            "token_type": self.token_type,
            "candidates" : self.candidates
        }



    def rotate_coordinates(self, page_height: int, rotation: int):
        rotated_token = replace(self)
        #TODO: extend for other rotations
        if rotation == 270:
            # page width & height also switched
            rotated_token  = Token(self.text,self.y0,page_height - self.x1,self.y1,page_height - self.x0, page_width=self.page_height, page_height=self.page_width)
        return rotated_token



def get_tokens(page):
    tokens = []

    rotation = page.rotation
    page_width = page.rect.width
    page_height = page.rect.height

    # Iterate over annotations
    for annot in page.annots():

        rect = annot.rect
        content = annot.info.get("content", "")

        #if content not in ["RV","107","08"]:
            #continue

        token = Token(
            content,
            x0=rect.x0,
            y0=rect.y0,
            x1=rect.x1,
            y1=rect.y1,
            page_width=page_width,
            page_height=page_height,
        ).rotate_coordinates(page_height, rotation)
        tokens.append(token)
    return tokens

def mark_pid_links(tokens):
    leftover_tokens =[]
    pid_links = []
    for t in tokens:
        if is_pid_link(t):
            t.token_type = "PID_LINK"
            pid_links.append(t)
            continue
        leftover_tokens.append(t)
    return leftover_tokens, pid_links

def mark_tokens_in_equipment_list(tokens, equipment_list_tags):
    tags = []
    leftover_tokens = []
    for t in tokens:
        if t.text.upper() in equipment_list_tags:
            t.token_type = "VALIDATED_BY_EQUIPMENTLIST"
            tags.append(t)
            continue
        leftover_tokens.append(t)
    return leftover_tokens, tags

def get_tokens_matching_part_of_equipment_list_item(tokens, equipment_list_tags, validated_tags):
    mapped_tag_dict = {}
    not_matching_tokens =[]
    used_equipment_list_tags = [item.text.upper() for item in validated_tags]

    for token in tokens:
        match_found = False
        for tag in equipment_list_tags:
            if tag in used_equipment_list_tags:
                continue

            if token.text.upper() in tag:
                if tag not in mapped_tag_dict.keys():
                    mapped_tag_dict[tag] = []

                mapped_tag_dict[tag].append(token)
                match_found = True

        if not match_found:
            not_matching_tokens.append(token)
    return not_matching_tokens, mapped_tag_dict

def find_combination(target, raw_group):
    chunks = [t.text for t in raw_group.get("members")]
    for perm in itertools.permutations(chunks):
        candidate = "".join(perm)
        if candidate == target:
            return candidate
    return None


def validate_grouped_token(raw_group, tag):
    valid_combination = find_combination(tag, raw_group)

    if not valid_combination:
        return  None
    else:
        raw_group["tag"] = valid_combination

    return raw_group

def group_mapped_tokens(mapped_token_dict: Dict):
    # Need to be careful. Tokens can be a candidate for multiple tags so it can be a leftover for one but a match for another.
    # we cannot just take the sum of all leftovers to determine the leftovers.

    possible_leftovers = []
    matched_tokens = []
    group_tokens = []
    for tag, candidates in mapped_token_dict.items():
        raw_groups, leftovers = group_tags(candidates)
        if len(raw_groups) == 1:
            raw_group = raw_groups[0]
        elif len(raw_groups) > 1:
            raise ValueError("multiple groups found")
        else:
            # No group found (TODO: handle this)
            possible_leftovers.extend(leftovers)
            continue


        validated_group = validate_grouped_token(raw_group, tag)
        members = raw_group.get("members")
        if validated_group:

            matched_tokens.extend(members)
            group_token = create_group_token(raw_group)
            group_token.token_type = "GROUPED_TOKEN_EQUIPMENT_LIST"
            group_tokens.append(group_token)
        else:
            leftovers.extend(members)

        possible_leftovers.extend(leftovers)


    matched_ids = [t.id for t in matched_tokens]
    print("matched ids", matched_ids)
    leftover_tokens = []
    seen_ids = []
    for l in list(possible_leftovers):
        if l.id in seen_ids:
            continue
        if l.id not in matched_ids:
            leftover_tokens.append(l)
        seen_ids.append(l.id)

    return group_tokens, leftover_tokens


def group_unmapped_tokens(tokens):
    group_tokens = []
    raw_groups, leftovers = group_tags(tokens)
    for raw_group in raw_groups:
        group_token = create_group_token(raw_group)
        group_token.token_type = "GROUPED_TOKEN_NEAREST_NEIGHBOUR"
        group_tokens.append(group_token)

    return group_tokens, leftovers


def is_pid_link(token: Token) -> bool:
    text = token.text
    regexes = config["pid_links"]["include_regexes"]
    match = False
    for r in regexes:
        result = re.match(r, text)
        if result:
            match = True
            break

    return match


def is_eligible_as_token(token):
    text = token.text

    tag_config = config["tags"]["raw"]

    # Check length of text
    if len(text) < tag_config["min_length"] or len(text) > tag_config["max_length"]:
        return False

    # Check position of tag
    if token.x0 < tag_config["min_padding_x"] or token.y0 < tag_config["min_padding_y"]:
        return False
    if token.x0 > token.page_width - tag_config["min_padding_x"] or token.y0 > token.page_height - tag_config["min_padding_y"]:
        return False

    # exclude tags based on regexes
    exclude_regexes = tag_config["exclude_regexes"]
    for r in exclude_regexes:
        if re.match(r, text):
            return False

    # exclude tags based on words
    wrong_texts = tag_config["wrong_texts"]
    for t in wrong_texts:
        if re.match(rf".*{t}.*", text, re.IGNORECASE):
            return False

    return True


def cleanup_tokens(tokens: List[Token]) -> Tuple[List[Token], List[Token]]:
    eligible_tokens = []
    discarded_tokens =[]
    for token in tokens:
        if is_eligible_as_token(token):
            eligible_tokens.append(token)
        else:
            discarded_tokens.append(token)
    return eligible_tokens, discarded_tokens


def token_type(t: Text) -> Text:
    if re.fullmatch(r"[A-Za-z]+", t): return "alpha"
    if re.fullmatch(r"\d+", t): return "num"
    return "alnum"


def same_line(a: Token, b: Token) -> Tuple[bool, Dict]:
    # vertical proximity allowing for font/rounding differences
    tolerance = config["tags"]["cleaned"]["same_line"]["base_tolerance"] * a.page_height
    tol = max(min(a.h, b.h), tolerance)
    result =  (b.cy - a.cy) > 0 and (b.cy - a.cy) < tol
    result_meta =  {
        "page_tolerance":tolerance,
        "scaled_tolerance": tol,
        "abs_cy": abs(b.cy-a.cy)
    }

    return result, result_meta

def near_right(a: Token, b: Token)-> Tuple[bool, Dict]:

    tolerance = config["tags"]["cleaned"]["near_right"]["base_tolerance"] * a.page_width
    # b is to the right of a (or slightly left, accounting for small misalignments)
    result = (b.cx - a.cx) >= -tolerance and (b.cx - a.cx) <= tolerance
    result_meta =  {
        "page_tolerance":tolerance,
        "scaled_tolerance": tolerance,
        "abs_cx": abs(b.cx-a.cx)
    }
    return result, result_meta

def create_group_token(group):
    members = group.get("members")
    candidates = {}
    for m in members:
        candidates[m.id] = {
            "text": m.text,
            "x0": m.x0,
            "y0": m.y0,
            "x1": m.x1,
            "y1": m.y1
        }
    member_0 = members[0]

    x = min([t.x0 for t in members])
    y = min([t.y0 for t in members])
    x1 = max([t.x1 for t in members])
    y1 = max([t.y1 for t in members])

    if "tag" in group.keys():
        text = group["tag"]
    else:
        text = "".join([t.text for t in members])

    grouped_token = Token(
        text=text,
        x0=x, y0=y,
        x1=x1, y1=y1,
        candidates=candidates,
        page_width=member_0.page_width,
        page_height=member_0.page_height
    )

    return grouped_token

def group_tags(tokens):
    # 1) classify

    items = [(t, token_type(t.text), t.id) for t in tokens]

    # 2) anchor on alpha tokens (prefix like PCV)
    groups = []
    used = set()

    for i, (tokA, typA, idA) in enumerate(items):
        ## WE LOOP OVER ALL TEXTUALS
        if typA not in ["alpha","alnum"] or tokA.id in used:
            continue

        # 3) find numeric neighbors on same line & near right
        nums = []
        for j, (tokB, typB,idB) in enumerate(items):
            # Skip the ones that we already used
            if j == i or tokB.id in used:
                continue
            # Check if they are neighours on x ad y-axis.

            same_line_result, sl_meta = same_line(tokA,tokB)
            near_right_result, nr_meta = near_right(tokA, tokB)
            if typB == "num" and same_line_result and near_right_result:
                nums.append(tokB)


        # If we found neighbours, we continue
        if nums:
            # sort numbers left-to-right and de-duplicate by value if very close
            nums.sort(key=lambda p: p.y0)

            num_vals = [tb.text for tb in nums]
            #tag = f"{tokA.text}{sep}{sep.join(num_vals)}"
            raw_group_token ={
                "members": [tokA] + [tb for tb in nums]
            }
            #group_token = create_group_token(raw_group_token)
            groups.append(raw_group_token)

            used.add(tokA.id)
            for tb in nums:
                used.add(tb.id)

    # 4) leftover standalone numbers/alphas if needed
    leftovers = [token for token in tokens if token.id not in used]
    return groups, leftovers



def split_matches_and_leftovers(tokens, regex):
    pattern = re.compile(regex)

    matches = [s for s in tokens if pattern.search(s.text)]
    leftovers = [s for s in tokens if not pattern.search(s.text)]

    return matches, leftovers


def extract_tags_from_leftovers(tokens):
    # Type 1: 2 or more CHARS + 2 or more Digits, ...
    regexes = config["tags"]["cleaned"]["mark_as_tag_regexes"]
    patterns = [re.compile(r) for r in regexes]

    leftovers = []
    matches = []

    for t in tokens:
        matched=False
        for p in patterns:
            if p.search(t.text):
                matches.append(t)
                t.token_type = "TOKEN_REGEX"
                matched = True
                break
        if not matched:
            leftovers.append(t)

    return matches, leftovers
