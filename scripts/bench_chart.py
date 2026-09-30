import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11})
fig, (a, b) = plt.subplots(1, 2, figsize=(12, 4.6), dpi=150)

shapes = ["constant\ngauge", "sparse\nchanges", "counter\n+0..3", "fleet mix\n(loadgen)", "noisy\ngauge", "raw\nfloat64+int64"]
vals = [0.42, 0.66, 1.25, 3.11, 6.67, 16]
cols = ["#2a6f97"] * 5 + ["#b0b7bf"]
bars = a.barh(shapes[::-1], vals[::-1], color=cols[::-1], height=0.62)
for bar, v in zip(bars, vals[::-1]):
    a.text(v + 0.25, bar.get_y() + bar.get_height() / 2, f"{v:g} B", va="center")
a.set_xlabel("bytes per sample")
a.set_xlim(0, 18.5)
a.set_title("Gorilla compression by series shape", loc="left", fontweight="bold")

labels = ["ingest, WAL\n(page cache)", "ingest, WAL\n+ fsync", "WAL replay\n(1 core)"]
v2 = [9.98, 8.72, 3.52]
bb = b.bar(labels, v2, color=["#2a6f97", "#2a6f97", "#61a5c2"], width=0.55)
for bar, v in zip(bb, v2):
    b.text(bar.get_x() + bar.get_width() / 2, v + 0.15, f"{v:g}M/s", ha="center")
b.set_ylabel("million samples / second")
b.set_ylim(0, 11.5)
b.set_title("Throughput, 100k series over HTTP", loc="left", fontweight="bold")

for ax in (a, b):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
fig.text(0.01, 0.01, "i7-13620H, Go 1.25. 16 HTTP writers (64 with fsync). Queries: 53k/s, p99 1.1 ms (1h window, 1m avg).",
         fontsize=9, color="#555")
plt.tight_layout(rect=(0, 0.04, 1, 1))
plt.savefig("docs/bench.png")
