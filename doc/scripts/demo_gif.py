"""
Animated demo of the Python client (doc/images/demo.gif), shown in the README.

For each example, the code is typed line by line, then the map it produces
appears. The borders are downloaded with the client (read-only).

    cd python-package/cartiflette
    uv run python ../../doc/scripts/demo_gif.py

The vintages 2025+ are read from the test location of the pipeline until
they are published: set PATH_WITHIN_BUCKET to "production" afterwards.
"""

from __future__ import annotations

import io
import re
import warnings
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import ListedColormap, LogNorm
from PIL import Image, ImageDraw, ImageFont

from cartiflette import carti_download

PATH_WITHIN_BUCKET = "production"
YEAR = 2026

DOC = Path(__file__).parents[1]
OUTPUT = DOC / "images" / "demo.gif"
LOGO = DOC / "images" / "logo.png"
FONT = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"
FONT_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"

WIDTH, HEIGHT = 1000, 520
CODE_WIDTH = 500
# Colours of the documentation ("gratin" palette) and of the code panel
GRATIN = "#B4540A"
CREME = "#FFF8EC"
CODE_BG = "#1f1b16"
SYNTAX = {
    "default": "#f3e9dc",
    "keyword": "#e6a15c",
    "string": "#a8d08d",
    "number": "#f0c674",
    "argument": "#8fbcd4",
    "comment": "#8a7f72",
}
TOKENS = re.compile(
    r"(?P<comment>#.*)"
    r"|(?P<string>\"[^\"]*\"|'[^']*')"
    r"|(?P<keyword>\b(?:from|import)\b)"
    r"|(?P<argument>\b\w+(?==))"
    r"|(?P<number>\b\d+\b)"
    r"|(?P<default>.)"
)

EXAMPLES = [
    {
        "title": "Départements, DROM rapprochés",
        "code": """from cartiflette import carti_download

france = carti_download(
    values="France",
    borders="DEPARTEMENT",
    filter_by="FRANCE_ENTIERE_DROM_RAPPROCHES",
    year=2026,
    simplification=50,
)
france.plot("POPULATION")""",
        "kwargs": {
            "values": "France",
            "borders": "DEPARTEMENT",
            "filter_by": "FRANCE_ENTIERE_DROM_RAPPROCHES",
            "simplification": 50,
        },
        "column": "POPULATION",
        "cmap": "YlOrBr",
        "log": True,
    },
    {
        "title": "Communes d'une région",
        "code": """occitanie = carti_download(
    values="76",
    borders="COMMUNE",
    filter_by="REGION",
    year=2026,
)
occitanie.plot("POPULATION")""",
        "kwargs": {"values": "76", "borders": "COMMUNE", "filter_by": "REGION"},
        "column": "POPULATION",
        "cmap": "YlOrBr",
        "log": True,
    },
    {
        "title": "Paris et petite couronne",
        "code": """petite_couronne = carti_download(
    values=["75", "92", "93", "94"],
    borders="COMMUNE_ARRONDISSEMENT",
    filter_by="DEPARTEMENT",
    year=2026,
)
petite_couronne.plot("INSEE_DEP")""",
        "kwargs": {
            "values": ["75", "92", "93", "94"],
            "borders": "COMMUNE_ARRONDISSEMENT",
            "filter_by": "DEPARTEMENT",
        },
        "column": "INSEE_DEP",
        "cmap": ListedColormap(["#7A3A08", "#E6A15C", "#B4540A", "#F3C78E"]),
        "log": False,
    },
    {
        "title": "Aires d'attraction des villes",
        "code": """aav = carti_download(
    values="France",
    borders="AIRE_ATTRACTION_VILLES",
    filter_by="FRANCE_ENTIERE_DROM_RAPPROCHES",
    year=2026,
    simplification=50,
)
aav.query("AAV2020 != '000'").plot("POPULATION")""",
        "kwargs": {
            "values": "France",
            "borders": "AIRE_ATTRACTION_VILLES",
            "filter_by": "FRANCE_ENTIERE_DROM_RAPPROCHES",
            "simplification": 50,
        },
        "query": "AAV2020 != '000'",
        "column": "POPULATION",
        "cmap": "YlOrBr",
        "log": True,
    },
]


def render_map(example: dict) -> tuple[Image.Image, int]:
    """The map of an example, as an image of the right panel size, and its
    number of polygons."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        gdf = carti_download(
            year=YEAR, path_within_bucket=PATH_WITHIN_BUCKET, **example["kwargs"]
        )
    if "query" in example:
        gdf = gdf.query(example["query"])
    size = (WIDTH - CODE_WIDTH) / 100, (HEIGHT - 60) / 100
    fig, ax = plt.subplots(figsize=size, dpi=100)
    fig.patch.set_facecolor(CREME)
    values = gdf[example["column"]]
    norm = (
        LogNorm(vmin=max(values.min(), 1), vmax=values.max())
        if example["log"]
        else None
    )
    gdf.assign(
        **{example["column"]: np.maximum(values, 1) if example["log"] else values}
    ).plot(
        example["column"],
        ax=ax,
        cmap=example["cmap"],
        norm=norm,
        edgecolor="white",
        linewidth=0.15 if len(gdf) > 1000 else 0.4,
        categorical=not example["log"],
    )
    ax.set_axis_off()
    ax.set_aspect(1 / np.cos(np.radians(gdf.total_bounds[1::2].mean())))
    fig.tight_layout(pad=0.3)
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", facecolor=CREME)
    plt.close(fig)
    return Image.open(buffer).convert("RGB"), len(gdf)


def draw_code(draw: ImageDraw.ImageDraw, lines: list[str], font) -> None:
    char_width = font.getlength("m")
    line_height = 27
    x0, y = 24, 100
    for line in lines:
        x = x0
        for match in TOKENS.finditer(line):
            text = match.group()
            draw.text((x, y), text, font=font, fill=SYNTAX[match.lastgroup])
            x += char_width * len(text)
        y += line_height


def frame(example: dict, n_lines: int, map_image: Image.Image | None) -> Image.Image:
    image = Image.new("RGB", (WIDTH, HEIGHT), CREME)
    draw = ImageDraw.Draw(image)
    # Code panel
    draw.rectangle((0, 0, CODE_WIDTH, HEIGHT), fill=CODE_BG)
    logo = Image.open(LOGO).convert("RGBA")
    logo.thumbnail((44, 44))
    image.paste(logo, (20, 18), logo)
    draw.text(
        (74, 22), "cartiflette", font=ImageFont.truetype(FONT_BOLD, 22), fill=CREME
    )
    draw.text(
        (74, 50),
        "pip install cartiflette",
        font=ImageFont.truetype(FONT, 13),
        fill=SYNTAX["comment"],
    )
    font = ImageFont.truetype(FONT, 15)
    code = example["code"].splitlines()
    draw_code(draw, code[:n_lines], font)
    # Map panel
    draw.text(
        (CODE_WIDTH + 24, 20),
        example["title"],
        font=ImageFont.truetype(FONT_BOLD, 20),
        fill=GRATIN,
    )
    if map_image is not None:
        image.paste(map_image, (CODE_WIDTH, 60))
    return image


def main() -> None:
    frames, durations = [], []
    for example in EXAMPLES:
        print(example["title"], flush=True)
        map_image, n_polygons = render_map(example)
        # The result, shown below the code with the map
        shown = {**example, "code": f"{example['code']}\n\n# {n_polygons} polygones"}
        n = len(example["code"].splitlines())
        for i in range(1, n + 1):
            frames.append(frame(shown, i, None))
            durations.append(90)
        frames.append(frame(shown, n + 2, map_image))
        durations.append(3200)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    frames[0].save(
        OUTPUT,
        save_all=True,
        append_images=frames[1:],
        duration=durations,
        loop=0,
        optimize=True,
    )
    print(f"{OUTPUT}: {OUTPUT.stat().st_size / 1e6:.1f} MB, {len(frames)} frames")


if __name__ == "__main__":
    main()
