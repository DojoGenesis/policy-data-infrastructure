"""
Pipeline DAG Animation
Policy Data Infrastructure — ADR-007 stats animation series

Narrative: The PDI data pipeline as a directed acyclic graph (DAG). 13 source
adapters feed data in parallel waves → normalize → PostGIS → AnalyzeStage →
narrative engine → HTMLCraft deliverable. Nodes light up sequentially;
parallel waves animate simultaneously.

Style: Night Shift palette — dark background (#0a0a0f), amber accent, system font.
No voiceover — text labels carry the explanation.
Target: 60-90 seconds, single scene with five phases.
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
SOURCE_COLOR = "#06b6d4"     # cyan — data sources
NORM_COLOR = "#8b5cf6"       # violet — normalize
STORE_COLOR = "#22c55e"      # green — PostGIS
ANALYZE_COLOR = "#f97316"    # orange — AnalyzeStage
NARRATIVE_COLOR = "#f59e0b"  # amber — narrative engine
OUTPUT_COLOR = "#ef4444"     # red — HTMLCraft
WAVE1_COLOR = "#3b82f6"      # blue — wave 1
WAVE2_COLOR = "#06b6d4"      # cyan — wave 2
WAVE3_COLOR = "#a78bfa"      # light violet — wave 3
MONO = "Menlo"

# ── DAG Definition ──────────────────────────────────────────────────────────
# 5 layers: Sources → Normalize → PostGIS → Analyze → Narrative → Output
# Source layer has 13 adapters in 3 parallel waves

WAVE1 = ["Census ACS", "TIGER", "BLS LAUS", "CDC PLACES"]
WAVE2 = ["EPA EJScreen", "USDA Food", "WI DPI", "IRS SOI"]
WAVE3 = ["HUD HCV", "NCES CCD", "DOJ UCR", "CMS Geo", "HRSA"]

# ── Helpers ──────────────────────────────────────────────────────────────────
def make_text(s, font_size=24, color=WHITE, weight=None):
    kwargs = {"font_size": font_size, "color": color, "font": MONO}
    if weight:
        kwargs["weight"] = weight
    return Text(s, **kwargs)

def make_node(label, color, width=2.2, height=0.6):
    """Create a labeled rectangle node."""
    rect = RoundedRectangle(
        width=width, height=height, corner_radius=0.12,
        fill_color=color, fill_opacity=0.3,
        stroke_color=color, stroke_width=1.5,
    )
    text = make_text(label, font_size=14, color=color)
    text.move_to(rect)
    return VGroup(rect, text)


class PipelineDAG(Scene):
    """Pipeline DAG: 13 adapters → normalize → PostGIS → analyze → output."""

    def construct(self):
        self.camera.background_color = BG

        self.phase1_sources()
        self.phase2_normalize()
        self.phase3_pipeline()
        self.phase4_flow()
        self.phase5_insight()

    def phase1_sources(self):
        """Show the 13 source adapters in 3 waves."""

        title = make_text("DATA SOURCES", font_size=36, color=WHITE, weight=BOLD)
        title.to_edge(UP, buff=0.6)
        self.play(Write(title), run_time=1.5)
        self.wait(0.5)

        subtitle = make_text(
            "13 federal and state data sources feed the pipeline",
            font_size=20, color=DIM,
        )
        subtitle.next_to(title, DOWN, buff=0.25)
        self.play(FadeIn(subtitle), run_time=1.0)
        self.wait(1.0)

        # ── 3 waves of source nodes ─────────────────────────────────────────
        wave_labels = [
            (WAVE1, WAVE1_COLOR, "WAVE 1", UP * 2.2),
            (WAVE2, WAVE2_COLOR, "WAVE 2", UP * 0.5),
            (WAVE3, WAVE3_COLOR, "WAVE 3", DOWN * 1.2),
        ]

        all_source_nodes = VGroup()
        wave_label_objs = VGroup()

        for sources, color, wave_name, y_shift in wave_labels:
            wave_label = make_text(wave_name, font_size=16, color=color)
            wave_label.shift(LEFT * 5.0 + y_shift)
            wave_label_objs.add(wave_label)

            row = VGroup()
            for i, name in enumerate(sources):
                node = make_node(name, color, width=1.6, height=0.45)
                node.shift(LEFT * 3.5 + (i - (len(sources) - 1) / 2) * 1.8 * RIGHT + y_shift)
                row.add(node)
                all_source_nodes.add(node)

        # Show wave labels
        self.play(
            LaggedStart(
                *[FadeIn(wl) for wl in wave_label_objs],
                lag_ratio=0.3, run_time=1.5,
            ),
        )
        self.wait(0.5)

        # Show source nodes wave by wave
        wave1_nodes = VGroup(*[n for i, n in enumerate(all_source_nodes) if i < len(WAVE1)])
        wave2_nodes = VGroup(*[n for i, n in enumerate(all_source_nodes) if len(WAVE1) <= i < len(WAVE1) + len(WAVE2)])
        wave3_nodes = VGroup(*[n for i, n in enumerate(all_source_nodes) if i >= len(WAVE1) + len(WAVE2)])

        for wave_nodes, wave_name in [(wave1_nodes, "Wave 1"), (wave2_nodes, "Wave 2"), (wave3_nodes, "Wave 3")]:
            self.play(
                LaggedStart(
                    *[FadeIn(n) for n in wave_nodes],
                    lag_ratio=0.15, run_time=1.5,
                ),
            )
            self.wait(0.3)

        self.wait(1.0)

        self.play(
            FadeOut(title), FadeOut(subtitle),
            run_time=0.8,
        )
        self.wait(0.3)

        self.source_nodes = all_source_nodes
        self.wave_label_objs = wave_label_objs

    def phase2_normalize(self):
        """Show the normalize layer below all sources."""

        phase_title = make_text("NORMALIZE", font_size=30, color=NORM_COLOR, weight=BOLD)
        phase_title.to_edge(UP, buff=0.6)
        self.play(Write(phase_title), run_time=1.5)
        self.wait(0.5)

        # ── Normalize node ──────────────────────────────────────────────────
        norm_node = make_node("NORMALIZE", NORM_COLOR, width=3.0, height=0.7)
        norm_node.shift(LEFT * 0.5 + DOWN * 2.3)

        # ── Arrows from each source to normalize ────────────────────────────
        arrows_to_norm = VGroup()
        for source in self.source_nodes:
            arrow = Arrow(
                start=source.get_bottom(),
                end=norm_node.get_top(),
                color=NORM_COLOR, stroke_width=1.2,
                buff=0.1, max_tip_length_to_length_ratio=0.08,
                stroke_opacity=0.4,
            )
            arrows_to_norm.add(arrow)

        self.play(
            FadeIn(norm_node),
            run_time=1.0,
        )
        self.wait(0.3)

        self.play(
            LaggedStart(
                *[Create(a) for a in arrows_to_norm],
                lag_ratio=0.03, run_time=2.0,
            ),
        )
        self.wait(1.0)

        norm_note = make_text(
            "Standardize formats, validate schemas, detect anomalies",
            font_size=16, color=DIM,
        )
        norm_note.next_to(norm_node, DOWN, buff=0.4)
        self.play(FadeIn(norm_note), run_time=1.0)
        self.wait(1.5)

        self.play(
            FadeOut(phase_title), FadeOut(norm_note),
            run_time=0.8,
        )
        self.wait(0.3)

        self.norm_node = norm_node
        self.arrows_to_norm = arrows_to_norm

    def phase3_pipeline(self):
        """Reveal the full pipeline: PostGIS → AnalyzeStage → Narrative → HTMLCraft."""

        phase_title = make_text("PIPELINE DAG", font_size=30, color=AMBER, weight=BOLD)
        phase_title.to_edge(UP, buff=0.6)
        self.play(Write(phase_title), run_time=1.5)
        self.wait(0.5)

        # ── Build remaining nodes ───────────────────────────────────────────
        # PostGIS
        postgis_node = make_node("PostGIS", STORE_COLOR, width=2.8, height=0.7)
        postgis_node.shift(RIGHT * 3.8 + DOWN * 2.3)

        # AnalyzeStage
        analyze_node = make_node("AnalyzeStage", ANALYZE_COLOR, width=2.6, height=0.7)
        analyze_node.shift(RIGHT * 3.8 + DOWN * 1.0)

        # Narrative
        narrative_node = make_node("Narrative\nEngine", NARRATIVE_COLOR, width=2.6, height=0.7)
        narrative_node.shift(RIGHT * 3.8 + UP * 0.3)

        # HTMLCraft
        output_node = make_node("HTMLCraft", OUTPUT_COLOR, width=2.6, height=0.7)
        output_node.shift(RIGHT * 3.8 + UP * 1.6)

        # ── Arrows between pipeline stages ──────────────────────────────────
        arr_norm_postgis = Arrow(
            start=self.norm_node.get_right(),
            end=postgis_node.get_left(),
            color=STORE_COLOR, stroke_width=2, buff=0.15,
        )
        arr_postgis_analyze = Arrow(
            start=postgis_node.get_top(),
            end=analyze_node.get_bottom(),
            color=ANALYZE_COLOR, stroke_width=2, buff=0.1,
        )
        arr_analyze_narrative = Arrow(
            start=analyze_node.get_top(),
            end=narrative_node.get_bottom(),
            color=NARRATIVE_COLOR, stroke_width=2, buff=0.1,
        )
        arr_narrative_output = Arrow(
            start=narrative_node.get_top(),
            end=output_node.get_bottom(),
            color=OUTPUT_COLOR, stroke_width=2, buff=0.1,
        )

        # Animate pipeline nodes appearing right-to-left (output first, then back)
        self.play(
            FadeIn(output_node), FadeIn(narrative_node),
            run_time=1.0,
        )
        self.play(
            Create(arr_narrative_output),
            run_time=0.8,
        )
        self.wait(0.3)

        self.play(
            FadeIn(analyze_node),
            run_time=1.0,
        )
        self.play(
            Create(arr_analyze_narrative),
            run_time=0.8,
        )
        self.wait(0.3)

        self.play(
            FadeIn(postgis_node),
            run_time=1.0,
        )
        self.play(
            Create(arr_postgis_analyze), Create(arr_norm_postgis),
            run_time=1.0,
        )
        self.wait(1.0)

        # ── Layer labels ────────────────────────────────────────────────────
        stage_labels = VGroup()
        stages = [
            (postgis_node, "STORE", STORE_COLOR, RIGHT),
            (analyze_node, "ANALYZE", ANALYZE_COLOR, RIGHT),
            (narrative_node, "NARRATE", NARRATIVE_COLOR, RIGHT),
            (output_node, "DELIVER", OUTPUT_COLOR, RIGHT),
        ]
        for node, label, color, direction in stages:
            lbl = make_text(label, font_size=14, color=color)
            lbl.next_to(node, direction, buff=0.3)
            stage_labels.add(lbl)

        self.play(
            LaggedStart(
                *[FadeIn(l) for l in stage_labels],
                lag_ratio=0.3, run_time=1.5,
            ),
        )
        self.wait(1.5)

        self.play(FadeOut(phase_title), run_time=0.8)
        self.wait(0.3)

        self.postgis_node = postgis_node
        self.analyze_node = analyze_node
        self.narrative_node = narrative_node
        self.output_node = output_node
        self.pipeline_arrows = VGroup(arr_norm_postgis, arr_postgis_analyze, arr_analyze_narrative, arr_narrative_output)
        self.stage_labels = stage_labels

    def phase4_flow(self):
        """Animate the data flow through the pipeline — nodes light up sequentially."""

        phase_title = make_text("DATA FLOW", font_size=30, color=AMBER, weight=BOLD)
        phase_title.to_edge(UP, buff=0.6)
        self.play(Write(phase_title), run_time=1.5)
        self.wait(0.5)

        # ── Sequential glow animation ───────────────────────────────────────
        flow_order = [
            (VGroup(*self.source_nodes), SOURCE_COLOR, "13 adapters fetch data"),
            (self.norm_node, NORM_COLOR, "Normalize & validate"),
            (self.postgis_node, STORE_COLOR, "Store in PostGIS"),
            (self.analyze_node, ANALYZE_COLOR, "Statistical analysis"),
            (self.narrative_node, NARRATIVE_COLOR, "Narrative generation"),
            (self.output_node, OUTPUT_COLOR, "HTMLCraft deliverable"),
        ]

        for node, color, desc in flow_order:
            # Flash the node
            self.play(
                node.animate.set_fill(opacity=0.6),
                run_time=0.3,
            )

            desc_label = make_text(desc, font_size=16, color=color)
            desc_label.to_edge(DOWN, buff=0.6)
            self.play(FadeIn(desc_label), run_time=0.5)
            self.wait(0.5)

            self.play(
                node.animate.set_fill(opacity=0.3),
                FadeOut(desc_label),
                run_time=0.3,
            )

        # ── Highlight parallel waves ────────────────────────────────────────
        parallel_note = make_text(
            "Sources run in parallel waves for throughput",
            font_size=18, color=WAVE1_COLOR,
        )
        parallel_note.to_edge(DOWN, buff=0.6)
        self.play(FadeIn(parallel_note), run_time=1.0)

        # Flash each wave in parallel
        w1 = VGroup(*self.source_nodes[:4])
        w2 = VGroup(*self.source_nodes[4:8])
        w3 = VGroup(*self.source_nodes[8:])

        for wave, color in [(w1, WAVE1_COLOR), (w2, WAVE2_COLOR), (w3, WAVE3_COLOR)]:
            self.play(
                wave.animate.set_fill(opacity=0.7),
                run_time=0.4,
            )
            self.wait(0.2)
            self.play(
                wave.animate.set_fill(opacity=0.3),
                run_time=0.3,
            )

        self.wait(1.0)

        self.play(
            FadeOut(phase_title), FadeOut(parallel_note),
            run_time=0.8,
        )
        self.wait(0.3)

    def phase5_insight(self):
        """Key insight: the pipeline is a DAG — parallel where possible, sequential where necessary."""

        insight_title = make_text(
            "PIPELINE ARCHITECTURE",
            font_size=30, color=AMBER, weight=BOLD,
        )
        insight_title.to_edge(UP, buff=0.4)
        self.play(Write(insight_title), run_time=1.5)
        self.wait(0.5)

        takeaways = VGroup(
            make_text(
                "1. 13 source adapters run independently in waves",
                font_size=20, color=WHITE,
            ),
            make_text(
                "2. Normalize → Store → Analyze → Narrate → Deliver",
                font_size=20, color=WHITE,
            ),
            make_text(
                "3. Parallel at the edges, sequential through the core",
                font_size=20, color=WHITE,
            ),
        )
        takeaways.arrange(DOWN, buff=0.25, aligned_edge=LEFT)
        takeaways.shift(UP * 0.3)

        self.play(
            LaggedStart(
                *[FadeIn(t, shift=RIGHT * 0.3) for t in takeaways],
                lag_ratio=0.3,
                run_time=2.5,
            ),
        )
        self.wait(2.0)

        final_note = make_text(
            "From raw data to policy insights — one pipeline.",
            font_size=20, color=AMBER,
        )
        final_note.next_to(takeaways, DOWN, buff=0.5)
        self.play(FadeIn(final_note), run_time=1.5)
        self.wait(2.0)

        self.play(
            FadeOut(Group(*self.mobjects)),
            run_time=1.0,
        )
        self.wait(0.3)
