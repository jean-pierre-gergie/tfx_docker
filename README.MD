Known Issues 
No Id Provided 
this issue is due to the wrong format fo the comand 
docker run --rm -it --network tfx-docker-scratch_default  -v "$PWD\data:/data" apache/beam_python3.10_sdk:2.59.0 python -m apache_beam.examples.wordcount --input /data/words.txt --output /data/out --runner PortableRunner --job_endpoint beam-jobserver:8099 --artifact_endpoint beam-jobserver:8098 --environment_type DOCKER --environment_config '{"container_image":"apache/beam_python3.10_sdk:2.59.0"}'


--environment_config this is the correct parameter to set for docker container 


2- Container starting not doing the actual job 

when the job is delegated to spark master each spark worker will create a new container (Docker inside Docker)
the newly added container is trying to connect to local host which is wrong 
it needs to connect to the tfx-docker-scratch_default

so the main issue is to have --format "NetworkMode={{.HostConfig.NetworkMode}} output tfx-docker-scratch_default

at the moment it is ouputing host