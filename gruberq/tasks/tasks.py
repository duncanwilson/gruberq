from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
import requests
import os
import csv
import json as jsonx

#Default base directory 
basedir="/data/static/"

#Example task
@task()
def add(x, y):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    result = x + y
    return result

@task()
def add_usingR(x,y):
    task_id = str(add_usingR.request.id)
    sum = x + y
    docker_cmd = ' /bin/sh -c exit; '
    try:
        result = docker_task(docker_name="python",docker_opts=None,docker_command=docker_cmd,id=task_id)
        return result
    except:
        raise

    
    
@task()
def runRscript_file(args):
    """
        Generic task to batch submit to R
        args: run parameters saved as a text file in json format
              The run parameters will be read into R as part of the R script
              Users will need to know structure of the parms file
        kwargs: keyword arg is the R script filename
    """
    task_id = str(runRscript_file.request.id)
    resultDir = setup_result_directory(task_id)
    #host_data_resultDir = "{0}/static/someapp_tasks/{1}".format(host_data_dir,task_id)
    with open(resultDir + '/input/args.json', "w") as f:
        jsonx.dump(args,f)`
    #Run R Script
    docker_opts = " --rm -v /opt/someapp/data/static:/script:z -w /script "
    docker_cmd =" Rscript /script/simple.R "
    try:
        result = docker_task(docker_name="gruber_r",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
    except:
        pass
    result_url ="http://{0}/someapp_tasks/{1}".format("cybercom-dev.tigr.cf",task_id)
    return result_url

	
def setup_result_directory(task_id):
    resultDir = os.path.join(basedir, 'someapp_tasks/', task_id)
    os.makedirs(resultDir)
    os.makedirs("{0}/input".format(resultDir))
    os.makedirs("{0}/output".format(resultDir))
    return resultDir 

