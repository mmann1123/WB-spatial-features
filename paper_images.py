# %%
from matplotlib import patches
import matplotlib.pyplot as plt

# Create figure and axes
fig, ax = plt.subplots()

# Add the outermost rectangle (Scale size 7 = 70m)
outer_rect = patches.Rectangle(
    (0, 0), 70, 70, linewidth=2, edgecolor="red", facecolor="none"
)
ax.add_patch(outer_rect)

# Add the middle rectangle (Scale size 5 = 50m)
middle_rect = patches.Rectangle(
    (10, 10), 50, 50, linewidth=2, edgecolor="blue", facecolor="none"
)
ax.add_patch(middle_rect)

# Add the inner rectangle (Scale size 3 = 30m)
inner_rect = patches.Rectangle(
    (20, 20), 30, 30, linewidth=2, edgecolor="green", facecolor="none"
)
ax.add_patch(inner_rect)

# Add the innermost rectangle (Block and Pixel size = 10m)
innermost_rect = patches.Rectangle(
    (30, 30), 10, 10, linewidth=2, edgecolor="black", facecolor="none"
)
ax.add_patch(innermost_rect)

# Add the grid within the red box
for i in range(8):
    if i <= 7:
        plt.axhline(i * 10, color="lightgrey", linewidth=0.5)
        plt.axvline(i * 10, color="lightgrey", linewidth=0.5)

# Add labels to the rectangles
ax.text(
    35, 35, "A", ha="center", va="center", fontsize=12, color="black", weight="bold"
)
ax.text(45, 25, "B", ha="right", va="bottom", fontsize=12, color="green", weight="bold")
ax.text(55, 15, "C", ha="right", va="bottom", fontsize=12, color="blue", weight="bold")
ax.text(65, 5, "D", ha="right", va="bottom", fontsize=12, color="red", weight="bold")

# Add the annotations
annotations = [
    "A) Block and Pixel size = 10m",
    "B) Scale size 3 = 30m",
    "C) Scale size 5 = 50m",
    "D) Scale size 7 = 70m",
]

for idx, annotation in enumerate(annotations):
    ax.text(80, 60 - 15 * idx, annotation, fontsize=12, verticalalignment="top")

# Set the limits and aspect ratio
ax.set_xlim(0, 70)
ax.set_ylim(0, 70)
ax.set_aspect("equal")

# Remove the axes
ax.axis("off")

# Show the plot
plt.show()


# %%
