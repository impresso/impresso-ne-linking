cluster-scheduler:
	screen -dmS dask-sched-masterdb dask-scheduler --port=8786

cluster-workers:
	screen -dmS dask-work-masterdb dask-worker localhost:8786 --nprocs 24 --nthreads 1 --memory-limit 2G
	# screen -dmS dask-work-masterdb dask-worker localhost:8786 --nprocs 36 --nthreads 1 --memory-limit 1G --local-directory=/scratch/matteo/dask-space

dask-cluster: cluster-scheduler cluster-workers


aida-gateway:
	screen -dmS aida-gateway java -jar lib/aida-impresso.jar
