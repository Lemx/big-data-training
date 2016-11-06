import psutil
import time
import csv

from collections import OrderedDict

import sys

run_count = 0
results = OrderedDict()

largeMemoryOpts = ["4g", "8g"]  # 2g isn't viable
mediumMemoryOptions = ["512m", "1g", "2g"]
smallMemoryOpts = ["1m", "2m", "4m"]
ratioOpts = [25, 50, 75]
threadsOpts = [2, 4, 8]
thresholdOpts = [1, 100, 10000]
optNames = ["-Xmn{}",
            "-Xms{} -Xmx{}",
            "-XX:ThreadStackSize={}",
            "-XX:SurvivorRatio={}",
            "-XX:CompileThreshold={}",
            "-XX:CICompilerCount={}",
            "-XX:ParallelGCThreads={}"]
opts = dict(zip(optNames, [mediumMemoryOptions, largeMemoryOpts, smallMemoryOpts,
                           ratioOpts, thresholdOpts, threadsOpts, threadsOpts]))

mean = lambda x: sum(x) / len(x) if x else 0
safeMax = lambda x: max(x) if x else 0


def dict_to_csv(dictionary, csvfile):
    with open(csvfile, "w") as f:
        w = csv.writer(f)
        w.writerow(["run", "args", "time", "maxCpu", "meanCpu", "maxMem", "meanMem"])
        for item in dictionary.items():
            w.writerow([item[1][0],
                        item[0],
                        item[1][1],
                        item[1][2],
                        round(item[1][3], 2),
                        round(item[1][4] / 1024 ** 3, 2),
                        round(item[1][5] / 1024 ** 3, 2)])


for val0 in opts[optNames[0]]:
    for val1 in opts[optNames[1]]:
        for val2 in opts[optNames[2]]:
            for val3 in opts[optNames[3]]:
                for val4 in opts[optNames[4]]:
                    for val5 in opts[optNames[5]]:
                        for val6 in opts[optNames[6]]:
                            args = [optNames[0].format(val0),
                                    *optNames[1].format(val1, val1).split(" "),
                                    optNames[2].format(val2),
                                    optNames[3].format(val3),
                                    optNames[4].format(val4),
                                    optNames[5].format(val5),
                                    optNames[6].format(val6),
                                    "-XX:+AggressiveOpts",
                                    "-XX:+UseParallelGC"]
                            key = " ".join(args)
                            intervals = 0
                            mem = []
                            cpu = []
                            try:
                                p = psutil.Popen(["java", *args,
                                                  "-jar", sys.argv[2], *sys.argv[3:]])
                                while p.poll() is None:
                                    mem.append(p.memory_info().rss)
                                    cpu.append(p.cpu_percent(None))
                                    time.sleep(1)
                                    intervals += 1
                                cpu = [x for x in cpu if x > 0.0]
                            except psutil.ZombieProcess as ex:
                                print(ex)
                                continue
                            finally:
                                run_count += 1
                                results[key] = (run_count, intervals, safeMax(cpu), mean(cpu), safeMax(mem), mean(mem))

dict_to_csv(results, sys.argv[1])
