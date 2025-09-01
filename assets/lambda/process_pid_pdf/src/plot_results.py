from data.pid_tag import pid_tag as data_pid_tag
from data.pid_file_page import pid_file_page as data_pid_file_page
import matplotlib.pyplot as plt
import matplotlib.patches as patches

from core.database.db import Session as db

with db() as session:
    ## FILL IN THE pid_file_pag_id &
    all_tags  = data_pid_tag.get_all(limit=1500,db=session, pid_file_page_id = 2)
    page = data_pid_file_page.from_id(id=2)

fig, ax = plt.subplots(figsize=(30,24))

# Draw tokens
for tag in all_tags:
    if tag.type =="RAW":
        continue

    if tag.type == "DISCARDED_TOKENS":
        continue
    print(tag.id, tag.tag_value)

    color = "blue"
    if tag.type == "VALIDATED":
        if tag.sub_type == "TOKEN_REGEX":
            color = "blue"

        elif tag.sub_type == "VALIDATED_BY_EQUIPMENTLIST":
            color = "green"


        elif tag.sub_type == "GROUPED_TOKEN_NEAREST_NEIGHBOUR":
            color = "black"

        elif tag.sub_type =="GROUPED_TOKEN_EQUIPMENT_LIST":

            color = "purple"
    else:
        color = "red"



    cx = (tag.x0 + tag.x1) / 2
    cy = (tag.y0 + tag.y1) / 2

    ax.text(cx, cy, tag.tag_value, ha='center', va='center', fontsize=8, color=color,)

blue_patch = patches.Patch(color="blue", label="TOKEN REGEX")
green_patch = patches.Patch(color="green", label="VALIDATED_BY_EQUIPMENTLIST")
black_patch = patches.Patch(color="black", label="GROUPED_TOKEN_NEAREST_NEIGHBOUR")
purple_patch = patches.Patch(color="purple", label="GROUPED_TOKEN_EQUIPMENT_LIST")
ax.legend(handles=[blue_patch, green_patch, black_patch, purple_patch], title="Tag Type")


# Adjust axes to show all tokens
all_x = [t.x0 for t in all_tags] + [t.x1 for t in all_tags]
all_y = [t.y0 for t in all_tags] + [t.y1 for t in all_tags]
ax.set_xlim(0, page.width)
ax.set_ylim(page.height,0)  # invert y-axis to match PDF

ax.set_aspect('equal')
ax.set_xlabel("x")
ax.set_ylabel("y")
ax.set_title("Token positions on PDF page")



plt.show()



