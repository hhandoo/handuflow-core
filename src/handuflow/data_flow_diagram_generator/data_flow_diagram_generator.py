# inbuilt
import os
import logging
import configparser
from collections import defaultdict

# external
import matplotlib

import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle, FancyArrow
import pandas as pd

from handuflow.config.config_paths import cfg_get


class DataFlowDiagramGenerator:

    def __init__(self, validated_dataframe: pd.DataFrame, config: configparser.ConfigParser, run_id: str) -> None:
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        self.BOX_WIDTH = float(config['LINEAGE_DIAGRAM']['BOX_WIDTH'])
        self.BOX_HEIGHT = float(config['LINEAGE_DIAGRAM']['BOX_HEIGHT'])
        self.X_GAP = float(config['LINEAGE_DIAGRAM']['X_GAP'])
        self.Y_GAP = float(config['LINEAGE_DIAGRAM']['Y_GAP'])
        self.ROOT_GAP = float(config['LINEAGE_DIAGRAM']['ROOT_GAP'])
        self.validated_dataframe = validated_dataframe
        self.name_map = {}
        self.current_y = 0
        self.positions = {}
        self.lineage_bounds = []
        self.roots = None
        self.lineage_id = 1
        matplotlib.use("Agg")


    def run(self):
        if len(self.validated_dataframe) != 0:
            self.logger.info('Building tree structure...')
            self.__build_tree_structure()
            self.logger.info('Layout each root separately...')
            self.__layout_root()
            self.logger.info('Plot...')
            self.__plot()
            self.logger.info('Draw lineage separators & labels...')
            self.__lineage_separators_and_lables()
            self.logger.info('Final cleanup and generate files...')
            self.__title_cleanup_and_save()

    def __build_tree_structure(self):
        self.children = defaultdict(list)
        self.name_map = dict(zip(self.validated_dataframe['feed_id'], self.validated_dataframe['feed_name']))
        for _, r in self.validated_dataframe.iterrows():
            self.children[r.parent_feed_id].append(r.feed_id)

    def __layout_subtree(self, node_id, depth):
        for child in self.children.get(node_id, []):
            self.__layout_subtree(child, depth + 1)
        if node_id not in self.positions:
            self.positions[node_id] = (
                depth * (self.BOX_WIDTH + self.X_GAP),
                self.current_y
            )
            self.current_y -= self.Y_GAP
        child_ys = [
            self.positions[c][1]
            for c in self.children.get(node_id, [])
            if c in self.positions
        ]
        if child_ys:
            avg_y = sum(child_ys) / len(child_ys)
            self.positions[node_id] = (
                depth * (self.BOX_WIDTH + self.X_GAP),
                avg_y
            )

    def __layout_root(self):
        self.roots = self.children[0]
        for root in self.roots:
            lineage_top = self.current_y
            self.__layout_subtree(root, 0)
            lineage_bottom = self.current_y
            self.lineage_bounds.append((lineage_top, lineage_bottom, self.lineage_id))
            self.lineage_id += 1
            self.current_y -= self.ROOT_GAP

    def __plot(self):
        fig, self.ax = plt.subplots(figsize=(80, max(10, len(self.validated_dataframe) * 2)))
        for fid, (x, y) in self.positions.items():
            if fid in self.roots:
                facecolor = "#C0392B"   # red
                text_color = "white"
            else:
                facecolor = "#D6EAF8"   # default blue
                text_color = "black"

            rect = Rectangle(
                (x, y),
                self.BOX_WIDTH,
                self.BOX_HEIGHT,
                edgecolor="black",
                facecolor=facecolor,
                linewidth=1.6
            )
            self.ax.add_patch(rect)

            label = f"{fid}: {self.name_map[fid]}"

            self.ax.text(
                x + self.BOX_WIDTH / 2,
                y + self.BOX_HEIGHT / 2,
                label,
                ha="center",
                va="center",
                fontsize=20,
                fontweight="bold",
                color=text_color,
                wrap=True
            )

        for _, r in self.validated_dataframe.iterrows():
            if r.parent_feed_id != 0:
                px, py = self.positions[r.parent_feed_id]
                cx, cy = self.positions[r.feed_id]

                arrow = FancyArrow(
                    px + self.BOX_WIDTH,
                    py + self.BOX_HEIGHT / 2,
                    cx - (px + self.BOX_WIDTH),
                    cy - py,
                    width=0.03,
                    head_width=0.4,
                    head_length=0.5,
                    length_includes_head=True,
                    color="black"
                )
                self.ax.add_patch(arrow)

    def __lineage_separators_and_lables(self):
        x_min = -2
        x_max = max(x for x, _ in self.positions.values()) + self.BOX_WIDTH + 2
        for top_y, bottom_y, lid in self.lineage_bounds:
            y_sep = bottom_y + self.ROOT_GAP / 2
            self.ax.hlines(
                y=y_sep,
                xmin=x_min,
                xmax=x_max,
                colors="gray",
                linestyles="dashed",
                linewidth=2
            )
            self.ax.text(
                x_min + 0.5,
                y_sep + 0.3,
                f"Lineage {lid}",
                fontsize=16,
                fontweight="bold",
                color="gray"
            )

    def __title_cleanup_and_save(self):
        self.ax.set_title(
            "HanduFlow - Feed Flow Diagram",
            fontsize=28,
            fontweight="bold",
            pad=30
        )
        self.ax.axis("off")
        self.ax.autoscale_view()
        path = os.path.join(
            cfg_get(self.config, "file_hunt_path"),
            cfg_get(self.config, "outbound_directory_name"),
            f"feed_lineage_{self.run_id}.png",
        )
        plt.savefig(path)
        plt.close()