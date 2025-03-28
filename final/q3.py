# -*- coding: utf-8 -*-
"""Outlier_Detection.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1Ly5ttRrYM0MlJlgZdYFZBSaje27yk5gW

#Outlier Detection

## Univariate Outlier Detection
"""

import pandas as pd
import seaborn as sns
from sklearn.preprocessing import StandardScaler, LabelEncoder
from itertools import combinations
from sklearn.cluster import DBSCAN
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

df = pd.read_csv('prog_book.csv')
print(df["Reviews"])
print(type(df["Reviews"][0]))
df["Reviews"] = df["Reviews"].str.replace(',', '').astype(int)
print(df["Reviews"])

print(df.head())

print(df["Type"].unique())

df[['Price', 'Number_Of_Pages', 'Rating', 'Reviews']] = df[['Price', 'Number_Of_Pages', 'Rating', 'Reviews']].apply(pd.to_numeric, errors='coerce')
df = df.dropna(subset=['Price', 'Number_Of_Pages', 'Rating', 'Reviews'])

vars = ['Price', 'Number_Of_Pages', 'Rating', 'Reviews']
for var in vars:
    plt.figure(figsize=(10, 4))
    q1, q3 = df[var].quantile([0.25, 0.75])
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    outliers = df[(df[var] < lower) | (df[var] > upper)][[var, 'Book_title']]
    sns.boxplot(x=df[var])
    plt.title(f'Box Plot of {var} (Outliers Highlighted)')
    plt.show()
    print(f"Outliers in {var}:")
    print(outliers, "\n")

"""## Multivariate Outlier Detection

### Bivariate Outliers Using DBSCAN (eps=0.8, min_samples=4)

#### Plotting Outliers and Combining non-outlier clusters
"""

from matplotlib.colors import ListedColormap

df['Type'] = df['Type'].astype(str)
df['Type'] = LabelEncoder().fit_transform(df['Type'])
df = df.dropna(subset=['Price', 'Number_Of_Pages', 'Rating', 'Reviews', 'Type'])

features = ['Price', 'Number_Of_Pages', 'Rating', 'Reviews', 'Type']

for combo in combinations(features, 2):
    X = df[list(combo)].values
    X_scaled = StandardScaler().fit_transform(X)

    db = DBSCAN(eps=0.8, min_samples=4)
    labels = db.fit_predict(X_scaled)
    vis_df = df.copy()
    vis_df['Cluster'] = labels

    plt.figure(figsize=(8, 6))

    sns.scatterplot(
        data=vis_df[vis_df['Cluster'] != -1],
        x=combo[0],
        y=combo[1],
        color='skyblue',
        label='Cluster'
    )

    sns.scatterplot(
        data=vis_df[vis_df['Cluster'] == -1],
        x=combo[0],
        y=combo[1],
        color='red',
        label='Outlier'
    )

    plt.title(f"DBSCAN (Outliers Highlighted): {combo[0]} vs {combo[1]}")
    plt.legend()
    plt.grid()
    plt.show()

    outliers = vis_df[vis_df['Cluster'] == -1]
    print(f"Outliers for {combo[0]} vs {combo[1]}:")
    print(outliers[[combo[0], combo[1]]].reset_index(), "\n")

"""### Three Variable Analysis (eps=1.4, min_samples=4)"""

for combo in combinations(features, 3):
    curr_data = df[list(combo)].values
    scaled = StandardScaler().fit_transform(curr_data)
    dbscan_model = DBSCAN(eps=1.4, min_samples=4)
    cluster_labels = dbscan_model.fit_predict(scaled)
    vis_df = df.copy()
    vis_df['Cluster'] = cluster_labels
    clustered = vis_df[vis_df['Cluster'] != -1]
    detected_outliers = vis_df[vis_df['Cluster'] == -1]
    fig = plt.figure(figsize=(10, 7))
    ax = fig.add_subplot(111, projection='3d')
    ax.scatter(
        clustered[combo[0]],
        clustered[combo[1]],
        clustered[combo[2]],
        c='skyblue',
        label='Cluster',
        alpha=0.7
    )

    ax.scatter(
        detected_outliers[combo[0]],
        detected_outliers[combo[1]],
        detected_outliers[combo[2]],
        c='red',
        label='Outlier',
        alpha=0.8
    )
    ax.set_xlabel(combo[0])
    ax.set_ylabel(combo[1])
    ax.set_zlabel(combo[2])
    plt.title(f"DBSCAN 3D Outliers: {combo[0]} vs {combo[1]} vs {combo[2]}")
    ax.legend()
    plt.show()

    print(f"Outliers for {combo[0]} vs {combo[1]} vs {combo[2]}:")
    print(detected_outliers[[combo[0], combo[1], combo[2]]].reset_index(), "\n")