import time
from pipeline.jobs.bronze import build_bronze_layer
# from pipeline.jobs.silver import build_silver_layer
# from pipeline.jobs.gold import build_gold_layer

if __name__ == "__main__":
    start_time = time.time()
    
    # 1. Bronze Phase
    build_bronze_layer()
    
    # 2. Silver Phase (TBD)
    # build_silver_layer()
    
    # 3. Gold Phase (TBD)
    # build_gold_layer()
    
    print(f"Total Execution Time: {round(time.time() - start_time, 2)} seconds")