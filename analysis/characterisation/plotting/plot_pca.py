import matplotlib.pyplot as plt
import polars as pl

def plot_pca_clusters(
    df,
    pc1="PC1",
    pc2="PC2",
    cluster_col="cluster",
    label_col="station",
    annotate=True,
    figsize=(6, 6),
    alpha=0.85,
    point_size=80,
):

    label_map = {0: "Cluster 0", 1: "Cluster 1", 2: "Cluster 2"}

    plt.figure(figsize=figsize)

    clusters = sorted(df[cluster_col].unique())

    for c in clusters:
        df_c = df.filter(pl.col(cluster_col) == c)

        plt.scatter(
            df_c[pc1].to_numpy(),
            df_c[pc2].to_numpy(),
            s=point_size,
            alpha=alpha,
            label=label_map.get(c, f"Cluster {c}"),
            edgecolors="black",
            linewidths=0.6,
        )

    if annotate:
        for row in df.iter_rows(named=True):
            plt.text(
                row[pc1] + 0.01, row[pc2] + 0.01, row[label_col], fontsize=8, alpha=0.8
            )

    plt.xlabel(pc1)
    plt.ylabel(pc2)
    plt.legend(frameon=True)
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.show()