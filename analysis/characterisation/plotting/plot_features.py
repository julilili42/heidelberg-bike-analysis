import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns

def plot_feature_boxplots(
    df, features, cluster_col="cluster", clusters=(0, 1), figsize=(4, 3)
):
    df_filt = df.filter(pl.col(cluster_col).is_in(clusters))

    for feat in features:
        plt.figure(figsize=figsize)
        sns.boxplot(data=df_filt, x=cluster_col, y=feat)
        plt.title(feat)
        plt.tight_layout()
        plt.show()

