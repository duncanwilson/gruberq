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
    """
        Generic task testing batch submit to R
            Adds two real numbers together
            Calls R from the docker container, inputs the args, writes result to file
            Return reads output file, returns sum
            Tests running R, inputting command args and writing output
        args: x,y are two real numbers
        file: "R_add_out.log" is the R console log
    """
    task_id = str(add_usingR.request.id)
    docker_opts = ' --rm -v /opt/someapp/data/static:/script:z -w /script '	
    docker_cmd = " Rscript /script/add_usingR.R {0} {1} ".format(x,y)
    try:
        r_return = docker_task(docker_name="gruber_r",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
    except:
        pass
    add_result = []
    # R returns a single row csv file with x,y,and x+y as columns
    with open('/data/static/R_add_out.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            add_result.append(row)
    f.close()
    #os.remove(basedir + 'R_add_out.csv')
    return add_result
    
    
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
        jsonx.dump(args,f)
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

