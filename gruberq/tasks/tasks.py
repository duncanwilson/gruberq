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
def add_usingR(x,y):
    """
        Generic task to sum two number with R
        args: x,y are real numbers
    """
    task_id = str(add_usingR.request.id)
    docker_cmd = "Rscript -e 'sum({0},{1})'".format(x,y)
    result = docker_task(docker_name="gruber_r",docker_opts=None,docker_command=docker_cmd,id=task_id)
    return result

@task()
def add_rpy2(x,y):
    """
        Generic task to sum two number with R via rpy2
        args: x,y are real numbers
    """
    task_id = str(runRscript_file.request.id)
    import rpy2.robjects as robjects
    from rpy2.robjects.packages import importr
    base = importr('base')
    rsum = robjects.r['sum']
    res = robjects.FloatVector([x,y])
    return rsum(res)[0]

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
    with open(resultDir + '/input/args.json', "w") as f:
        jsonx.dump(args,f)
    #Run R Script
    docker_opts = "-v /opt/someapp/data/static:/script -w /script"
    docker_cmd =" Rscript /script/simple.R "
    result = docker_task(docker_name="gruber_r",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
    result_url ="http://{0}/someapp_tasks/{1}".format("cybercom-dev.tigr.cf",task_id)
    return result_url
	
def setup_result_directory(task_id):
    resultDir = os.path.join(basedir, 'someapp_tasks/', task_id)
    os.makedirs(resultDir)
    os.makedirs("{0}/input".format(resultDir))
    os.makedirs("{0}/output".format(resultDir))
    return resultDir 

