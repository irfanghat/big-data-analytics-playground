import matplotlib.pyplot as plt
import numpy as np
import os

os.makedirs("results/figures", exist_ok=True)

# 1. Binary class distribution
labels = ["Normal", "Attack"]
counts = [2218456, 321283]

plt.figure(figsize=(8, 5))
plt.bar(labels, counts)
plt.title("UNSW-NB15 Binary Class Distribution")
plt.xlabel("Traffic Class")
plt.ylabel("Number of Records")
plt.savefig("results/figures/binary_distribution.png", dpi=300, bbox_inches="tight")
plt.close()

# 2. Attack category distribution
categories = [
    "Normal", "Generic", "Exploits", "Fuzzers", "DoS",
    "Reconnaissance", "Analysis", "Backdoors", "Shellcode", "Worms"
]
category_counts = [
    2218456, 215481, 44525, 24246, 16353,
    13987, 2677, 2329, 1511, 174
]

plt.figure(figsize=(11, 6))
plt.barh(categories[::-1], category_counts[::-1])
plt.title("UNSW-NB15 Traffic Distribution by Category")
plt.xlabel("Number of Records")
plt.ylabel("Category")
plt.savefig("results/figures/attack_categories.png", dpi=300, bbox_inches="tight")
plt.close()

# 3. Binary classification confusion matrix
binary_cm = np.array([
    [21942, 362],
    [34, 19777]
])

plt.figure(figsize=(6, 5))
plt.imshow(binary_cm, interpolation="nearest")
plt.title("Binary Classification Confusion Matrix")
plt.colorbar()
plt.xticks([0, 1], ["Normal", "Attack"])
plt.yticks([0, 1], ["Normal", "Attack"])
plt.xlabel("Predicted Label")
plt.ylabel("Actual Label")

for i in range(2):
    for j in range(2):
        plt.text(j, i, str(binary_cm[i, j]), ha="center", va="center")

plt.savefig("results/figures/binary_confusion_matrix.png", dpi=300, bbox_inches="tight")
plt.close()

# 4. Multiclass classification confusion matrix
# Label order: Exploits, Normal, DoS, Fuzzers, Generic,
# Reconnaissance, Analysis, Backdoors, Shellcode, Worms

multiclass_cm = np.array([
    [2846, 8, 0, 34, 0, 7, 1, 0, 5, 0],
    [7, 1330, 1052, 173, 6, 326, 2, 0, 16, 0],
    [13, 375, 2253, 116, 10, 141, 0, 0, 5, 0],
    [0, 60, 308, 2159, 12, 443, 1, 0, 0, 0],
    [1, 29, 26, 15, 2980, 3, 1, 0, 0, 0],
    [1, 6, 400, 334, 7, 1990, 0, 0, 0, 0],
    [0, 86, 390, 8, 0, 0, 30, 0, 0, 0],
    [1, 21, 375, 16, 0, 24, 0, 2, 7, 0],
    [0, 20, 0, 39, 0, 63, 0, 0, 174, 0],
    [0, 5, 1, 4, 0, 18, 0, 0, 0, 0]
])

class_names = [
    "Exploits", "Normal", "DoS", "Fuzzers", "Generic",
    "Recon.", "Analysis", "Backdoors", "Shellcode", "Worms"
]

plt.figure(figsize=(11, 9))
plt.imshow(multiclass_cm, interpolation="nearest")
plt.title("Multiclass Classification Confusion Matrix")
plt.colorbar()
plt.xticks(range(10), class_names, rotation=45, ha="right")
plt.yticks(range(10), class_names)
plt.xlabel("Predicted Category")
plt.ylabel("Actual Category")
plt.tight_layout()
plt.savefig("results/figures/multiclass_confusion_matrix.png", dpi=300, bbox_inches="tight")
plt.close()

# 5. Binary vs multiclass model performance
metrics = ["Accuracy", "Precision", "Recall", "F1-score"]
binary = [99.06, 99.07, 99.06, 99.06]
multiclass = [73.27, 76.78, 73.27, 71.90]

x = np.arange(len(metrics))
width = 0.35

plt.figure(figsize=(9, 5))
plt.bar(x - width/2, binary, width, label="Binary Model")
plt.bar(x + width/2, multiclass, width, label="Multiclass Model")
plt.xticks(x, metrics)
plt.ylabel("Percentage (%)")
plt.ylim(0, 105)
plt.title("Binary vs Multiclass Classification Performance")
plt.legend()
plt.savefig("results/figures/model_performance.png", dpi=300, bbox_inches="tight")
plt.close()

print("Charts generated successfully:")
for filename in os.listdir("results/figures"):
    print("results/figures/" + filename)
