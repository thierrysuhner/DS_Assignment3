# 1 Core:
# 1177ms
# 1067ms
# 1028ms
# 1041ms
# 1095ms

# 2 Cores:
# 1149ms
# 1004ms
# 1036ms
# 1027ms
# 1015ms

# 3 Cores:
# 1023ms
# 1015ms
# 1026ms
# 1052ms
# 1053ms

# 4 Cores:
# 1042ms
# 1077ms
# 1011ms
# 1093ms
# 1059ms

# 5 Cores:
# 1044ms
# 1039ms
# 1059ms
# 1033ms
# 1053ms

# 6 Cores:
# 1069ms
# 1041ms
# 1028ms
# 1027ms
# 1058ms

# 8 Cores:
# 1030ms
# 1069ms
# 1059ms
# 1069ms
# 1072ms

# 10 Cores:
# 1069ms
# 1177ms
# 1028ms
# 1129ms
# 1082ms



# No change is the learning – the division to the partition to the executors will always just be 1 executor -> no split up whatsoever 
# Do make histogramm, then this argumentation

import matplotlib.pyplot as plt
import numpy as np

# Timing data (in milliseconds)
times = {
    1: [1177, 1067, 1028, 1041, 1095],
    2: [1149, 1004, 1036, 1027, 1015],
    3: [1023, 1015, 1026, 1052, 1053],
    4: [1042, 1077, 1011, 1093, 1059],
    5: [1044, 1039, 1059, 1033, 1053],
    6: [1069, 1041, 1028, 1027, 1058],
    8: [1030, 1069, 1059, 1069, 1072],
    10: [1069, 1177, 1028, 1129, 1082]
}

# Compute mean and standard deviation
cores = sorted(times.keys())
means = [np.mean(times[c]) for c in cores]
stds = [np.std(times[c]) for c in cores]

# Plot
plt.figure(figsize=(8, 5))
plt.errorbar(cores, means, yerr=stds, fmt='-o', capsize=5)
plt.title('Execution Time vs Number of Cores')
plt.xlabel('Number of Cores')
plt.ylabel('Time (ms)')
plt.grid(True, linestyle='--', alpha=0.6)
plt.xticks(cores)
plt.tight_layout()
plt.show()