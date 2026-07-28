"""
LISA Cluster Map Animation
Policy Data Infrastructure — ADR-007 stats animation series

Narrative: Local Indicators of Spatial Autocorrelation (LISA) identify spatial
clusters — where high values neighbor high values (HH), low values neighbor low
(LL), or where outliers exist (HL, LH). Queen contiguity weights link neighbors.

Style: Night Shift palette — dark background (#0a0a0f), amber accent, system font.
No voiceover — text labels carry the explanation.
Target: 60-90 seconds, single scene with four phases.
"""

from manim import *
import numpy as np

# ── Palette ──────────────────────────────────────────────────────────────────
BG = "#0a0a0f"
AMBER = "#f59e0b"
GRAY = "#6b7280"
SUBTLE = "#3f3f46"
WHITE = "#e4e4e7"
DIM = "#52525b"
HH_COLOR = "#ef4444"     # red — High-High cluster
HL_COLOR = "#f472b6"     # pink — High-Low outlier
LH_COLOR = "#60a5fa"     # light blue — Low-High outlier
LL_COLOR = "#3b82f6"     # blue — Low-Low cluster
NS_COLOR = "#71717a"     # gray — not significant
MONO = "Menlo"

# ── Data ─────────────────────────────────────────────────────────────────────
# Wisconsin county centroids (simplified, relative coords)
# Representing ~25 counties in a WI-shaped arrangement
WI_COUNTIES = [
    # (x, y, value) — value determines LISA classification
    # Upper peninsula-ish / northern WI
    (1.0, 4.5, 0.2), (2.5, 4.5, 0.3), (4.0, 4.5, 0.25),
    (0.5, 3.8, 0.15), (2.0, 3.8, 0.4), (3.5, 3.8, 0.35), (5.0, 4.0, 0.3),
    (1.5, 3.2, 0.6), (3.0, 3.2, 0.55), (4.5, 3.2, 0.5),
    # Central belt
    (0.8, 2.5, 0.7), (2.2, 2.5, 0.65), (3.8, 2.5, 0.6), (5.2, 2.5, 0.55),
    (1.2, 1.8, 0.8), (2.8, 1.8, 0.75), (4.2, 1.8, 0.7),
    # Southern WI — high cluster
    (0.6, 1.1, 0.9), (2.0, 1.1, 0.85), (3.5, 1.1, 0.88), (5.0, 1.1, 0.92),
    (1.5, 0.5, 0.95), (3.0, 0.5, 0.9), (4.5, 0.5, 0.85),
]

# ── Helpers ──────────────────────────────────────────────────────────────────
def make_text(s, font_size=24, color=WHITE, weight=None):
    kwargs = {"font_size": font_size, "color": color, "font": MONO}
    if weight:
        kwargs["weight"] = weight
    return Text(s, **kwargs)


class LISAClusterMap(Scene):
    """LISA explainer: WI map → neighbor links → cluster colors → insight."""

    def construct(self):
        self.camera.background_color = BG

        self.phase1_map()
        self.phase2_weights()
        self.phase3_clusters()
        self.phase4_insight()

    def phase1_map(self):
        """Draw Wisconsin outline with county centroids."""

        title = make_text("SPATIAL PATTERNS", font_size=36, color=WHITE, weight=BOLD)
        title.to_edge(UP, buff=0.6)
        self.play(Write(title), run_time=1.5)
        self.wait(0.5)

        subtitle = make_text(
            "Where are the clusters? Are neighbors similar or different?",
            font_size=20, color=DIM,
        )
        subtitle.next_to(title, DOWN, buff=0.25)
        self.play(FadeIn(subtitle), run_time=1.0)
        self.wait(1.0)

        # ── Wisconsin outline ───────────────────────────────────────────────
        # Simplified WI shape as a polygon
        wi_outline_points = [
            (0.0, 0.0), (0.0, 5.2), (0.5, 5.5), (2.0, 5.5), (4.0, 5.2),
            (5.5, 5.0), (5.8, 4.0), (5.5, 2.5), (5.8, 1.0), (5.2, 0.0),
            (4.0, -0.3), (2.5, 0.0), (1.0, -0.1), (0.0, 0.0),
        ]

        # Scale to fit the frame
        scale_factor = 1.15
        wi_points = [(x * scale_factor - 2.8, y * scale_factor - 2.5, 0) for x, y in wi_outline_points]

        wi_outline = Polygon(
            *wi_points,
            color=SUBTLE, stroke_width=2.5,
            fill_color=BG, fill_opacity=0.3,
        )

        wi_label = make_text("WISCONSIN", font_size=20, color=GRAY)
        wi_label.next_to(wi_outline, UP, buff=0.3)

        self.play(
            Create(wi_outline), FadeIn(wi_label),
            run_time=2.0,
        )
        self.wait(1.0)

        # ── County centroids ────────────────────────────────────────────────
        centroids = VGroup()
        for x, y, val in WI_COUNTIES:
            sx = x * scale_factor - 2.8
            sy = y * scale_factor - 2.5
            dot = Dot(
                np.array([sx, sy, 0]),
                color=GRAY, radius=0.08,
            )
            centroids.add(dot)

        self.play(
            LaggedStart(
                *[GrowFromCenter(d) for d in centroids],
                lag_ratio=0.06,
                run_time=2.0,
            ),
        )
        self.wait(1.0)

        # ── Value label ─────────────────────────────────────────────────────
        value_note = make_text(
            "Each dot = a census tract with an indicator value",
            font_size=16, color=DIM,
        )
        value_note.shift(DOWN * 3.1)
        self.play(FadeIn(value_note), run_time=1.0)
        self.wait(1.5)

        self.play(
            FadeOut(title), FadeOut(subtitle), FadeOut(value_note),
            run_time=0.8,
        )
        self.wait(0.3)

        self.wi_outline = wi_outline
        self.centroids = centroids
        self.scale_factor = scale_factor

    def phase2_weights(self):
        """Show queen contiguity — neighbor links between adjacent tracts."""

        phase_title = make_text("QUEEN CONTIGUITY", font_size=30, color=AMBER, weight=BOLD)
        phase_title.to_edge(UP, buff=0.6)
        self.play(Write(phase_title), run_time=1.5)
        self.wait(0.5)

        explanation = make_text(
            "Tracts that share a border or corner are neighbors.",
            font_size=18, color=DIM,
        )
        explanation.next_to(phase_title, DOWN, buff=0.2)
        self.play(FadeIn(explanation), run_time=1.0)
        self.wait(1.0)

        # Build neighbor links (simplified: connect nearby centroids)
        # For each centroid, connect to its closest neighbors
        def dist(i, j):
            x1, y1, _ = WI_COUNTIES[i]
            x2, y2, _ = WI_COUNTIES[j]
            return np.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)

        links = VGroup()
        threshold = 1.6  # Distance threshold for "adjacency"
        for i in range(len(WI_COUNTIES)):
            for j in range(i + 1, len(WI_COUNTIES)):
                if dist(i, j) < threshold:
                    x1, y1, _ = WI_COUNTIES[i]
                    x2, y2, _ = WI_COUNTIES[j]
                    sx1 = x1 * self.scale_factor - 2.8
                    sy1 = y1 * self.scale_factor - 2.5
                    sx2 = x2 * self.scale_factor - 2.8
                    sy2 = y2 * self.scale_factor - 2.5

                    line = Line(
                        np.array([sx1, sy1, 0]),
                        np.array([sx2, sy2, 0]),
                        color=SUBTLE, stroke_width=0.8,
                        stroke_opacity=0.5,
                    )
                    links.add(line)

        # Animate links appearing — highlight one then all
        self.play(
            LaggedStart(
                *[Create(l, run_time=0.3) for l in links],
                lag_ratio=0.02,
                run_time=3.0,
            ),
        )
        self.wait(1.0)

        # ── Highlight a single link to explain ──────────────────────────────
        highlight_link = links[0].copy()
        highlight_link.set_color(AMBER).set_stroke(width=2)
        self.play(
            Flash(highlight_link, color=AMBER, line_length=0.3, flash_radius=0.3),
            run_time=1.0,
        )
        self.wait(0.5)

        link_note = make_text(
            "Each link = spatial weight → \"these two influence each other\"",
            font_size=16, color=AMBER,
        )
        link_note.shift(DOWN * 3.0)
        self.play(FadeIn(link_note), run_time=1.0)
        self.wait(1.5)

        self.play(
            FadeOut(phase_title), FadeOut(explanation), FadeOut(link_note),
            FadeOut(links),
            run_time=0.8,
        )
        self.wait(0.3)

        self.links = links

    def phase3_clusters(self):
        """Color-code centroids into LISA clusters: HH, LL, HL, LH."""

        phase_title = make_text("LISA CLUSTERS", font_size=30, color=AMBER, weight=BOLD)
        phase_title.to_edge(UP, buff=0.6)
        self.play(Write(phase_title), run_time=1.5)
        self.wait(0.5)

        subtitle = make_text(
            "HH = High value, High neighbors    LL = Low value, Low neighbors\n"
            "HL = High value, Low neighbors      LH = Low value, High neighbors",
            font_size=14, color=DIM,
        )
        subtitle.next_to(phase_title, DOWN, buff=0.2)
        self.play(FadeIn(subtitle), run_time=1.5)
        self.wait(1.0)

        # ── Assign LISA classes ─────────────────────────────────────────────
        # For demo: southern counties = HH, northern = LL, some HL/LH
        lisa_classes = []
        for x, y, val in WI_COUNTIES:
            if y < 1.5:
                cls = "HH"
            elif y > 3.5:
                cls = "LL"
            elif val > 0.5 and y < 2.5:
                cls = "HL"
            elif val < 0.5 and y > 2.5:
                cls = "LH"
            else:
                cls = "NS"
            lisa_classes.append(cls)

        class_colors = {"HH": HH_COLOR, "HL": HL_COLOR, "LH": LH_COLOR, "LL": LL_COLOR, "NS": NS_COLOR}

        # ── Color centroids ─────────────────────────────────────────────────
        colored_centroids = VGroup()
        for i, (centroid, cls) in enumerate(zip(self.centroids, lisa_classes)):
            new_dot = Dot(
                centroid.get_center(),
                color=class_colors[cls],
                radius=0.1,
            )
            colored_centroids.add(new_dot)

        # Transition from gray to colored
        self.play(
            *[ReplacementTransform(old, new)
              for old, new in zip(self.centroids, colored_centroids)],
            run_time=2.0,
        )
        self.wait(1.0)

        # Store updated centroids
        self.centroids = colored_centroids

        # ── Cluster region labels ───────────────────────────────────────────
        # Highlight the HH cluster region (south)
        hh_brace = Brace(
            VGroup(*[c for i, c in enumerate(colored_centroids) if lisa_classes[i] == "HH"]),
            DOWN, color=HH_COLOR,
        )
        hh_label = make_text("HH Cluster", font_size=16, color=HH_COLOR)
        hh_label.next_to(hh_brace, DOWN, buff=0.15)

        self.play(
            FadeIn(hh_brace), FadeIn(hh_label),
            run_time=1.0,
        )
        self.wait(0.5)

        # LL cluster (north)
        ll_brace = Brace(
            VGroup(*[c for i, c in enumerate(colored_centroids) if lisa_classes[i] == "LL"]),
            UP, color=LL_COLOR,
        )
        ll_label = make_text("LL Cluster", font_size=16, color=LL_COLOR)
        ll_label.next_to(ll_brace, UP, buff=0.15)

        self.play(
            FadeIn(ll_brace), FadeIn(ll_label),
            run_time=1.0,
        )
        self.wait(1.5)

        # ── Legend ──────────────────────────────────────────────────────────
        legend = VGroup()
        legend_items = [
            ("HH", "High-High", HH_COLOR),
            ("HL", "High-Low", HL_COLOR),
            ("LH", "Low-High", LH_COLOR),
            ("LL", "Low-Low", LL_COLOR),
        ]
        for i, (code, desc, col) in enumerate(legend_items):
            dot = Dot(color=col, radius=0.08)
            label = make_text(f"  {code} = {desc}", font_size=14, color=col)
            item = VGroup(dot, label)
            item.arrange(RIGHT, buff=0.1)
            legend.add(item)

        legend.arrange(DOWN, buff=0.15, aligned_edge=LEFT)
        legend.to_edge(RIGHT, buff=0.5).shift(DOWN * 1.5)

        self.play(
            LaggedStart(
                *[FadeIn(item) for item in legend],
                lag_ratio=0.3,
                run_time=2.0,
            ),
        )
        self.wait(1.5)

        self.play(
            FadeOut(phase_title), FadeOut(subtitle),
            FadeOut(hh_brace), FadeOut(hh_label),
            FadeOut(ll_brace), FadeOut(ll_label),
            run_time=0.8,
        )
        self.wait(0.3)

        self.legend = legend

    def phase4_insight(self):
        """Key insight: LISA reveals spatial clusters, not just values."""

        # Fade out legend
        self.play(FadeOut(self.legend), run_time=0.5)

        insight_title = make_text(
            "LISA CLUSTER CLASSIFICATION",
            font_size=30, color=AMBER, weight=BOLD,
        )
        insight_title.to_edge(UP, buff=0.4)
        self.play(Write(insight_title), run_time=1.5)
        self.wait(0.5)

        takeaways = VGroup(
            make_text(
                "1. Spatial autocorrelation — neighbors are not independent",
                font_size=20, color=WHITE,
            ),
            make_text(
                "2. Queen contiguity defines the spatial weights matrix",
                font_size=20, color=WHITE,
            ),
            make_text(
                "3. HH/LL clusters → spatial inequality; HL/LH → outliers",
                font_size=20, color=WHITE,
            ),
        )
        takeaways.arrange(DOWN, buff=0.25, aligned_edge=LEFT)
        takeaways.shift(UP * 0.2)

        self.play(
            LaggedStart(
                *[FadeIn(t, shift=RIGHT * 0.3) for t in takeaways],
                lag_ratio=0.3,
                run_time=2.5,
            ),
        )
        self.wait(2.0)

        final_note = make_text(
            "Spatial thinking reveals patterns invisible in tables.",
            font_size=20, color=AMBER,
        )
        final_note.shift(DOWN * 1.6)
        self.play(FadeIn(final_note), run_time=1.5)
        self.wait(2.0)

        self.play(
            FadeOut(Group(*self.mobjects)),
            run_time=1.0,
        )
        self.wait(0.3)
