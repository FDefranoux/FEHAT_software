import glob2
import os
import sys
import src.setup as setup
from src.job_utils import *
import src.io_operations as io_operations
import subprocess
import logging
import re
import itertools


import configparser
curr_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(curr_dir, 'config.ini')
config = configparser.ConfigParser()
config.read(config_path)

MAIN_DIRECTORY = os.path.abspath(os.path.dirname(__file__))
LOGGER = logging.getLogger(__name__)

#TODO: What to do with the DEBUG, MAXJOB arguments?
# if debug : change stdout stderr to a specific file

def main(indir, channel_ls=[], loop_ls=[], well_range='', cluster=None, outdir='./outdir', debug=False, args={}):


    
    experiment_name = os.path.basename(os.path.normpath(indir))
    experiment_id = '_'.join(experiment_name.split('/')[0:2])
    setup.config_logger(os.path.join(outdir, 'log'), ("logfile_" + experiment_id + ".log"), debug)
    LOGGER.info("##### Job dispatching #####")
    LOGGER.debug('Input directory ' + indir)
    LOGGER.debug('Output directory ' + outdir)

    nr_of_videos, channels, loops, wells = io_operations.extract_data(indir)
    
    # Extract Video metadata
    LOGGER.info("Deduced number of videos: " + str(nr_of_videos))
    LOGGER.info("Deduced Channels: " + ', '.join(channels))
    LOGGER.info("Deduced number of Loops: " + str(len(loops)))

    max_subprocesses = int(config['DEFAULT']['MAX_PARALLEL_DIRS'])

    #QUESTION: Why is it not in the process argument part ?
    if channel_ls:
        channels = list(channel_ls.intersection(channels))
        channels.sort()
    if loop_ls:
        loops = list(loop_ls.intersection(loops))
        loops.sort()
    if not well_range:
        int_wells = [int(w.replace('WE', '')) for w in wells]
        well_array = str([min(int_wells), max(int_wells)]).replace(', ', '-')
    else:
        well_array = well_range
        
    
    filtered_info = io_operations.extract_data(indir, channel_ls=channels, loop_ls=loops, well_range=well_range)
    LOGGER.info("Filtered number of videos: " + str(filtered_info[0]))
    LOGGER.info("Filtered Channels: " + ', '.join(filtered_info[1]))
    LOGGER.info("Filtered number of Loops: " + str(len(filtered_info[2])))
    LOGGER.info("Filtered number of Wells: " + str(len(filtered_info[3])))

    assert (len(loops) > 0) or (len(channels) > 0), "No loops or channels were found!"
    combination = [dict(channels=c, loops=p) for c in channels for p in loops]
    LOGGER.debug('The list of combination are: \n')
    LOGGER.debug('\n'.join([str(c) for c in combination]))

    
    ################# PREPARE THE RUNS ######################
    python_cmd_ls = []
    job_ids_ls = []
    for comb in combination:
        comb_args =vars(args)
        comb_args.update(comb)
        bpm_python_cmd = prepare_python_cmd(comb_args, "medaka_bpm.py")
        if cluster :
            defaults_cluster_kwargs = dict(script=bpm_python_cmd,
                                           walltime='2:00:00', 
                                           jobname=f"HR_Analysis_{comb_args['loops']}_{comb_args['channels']}",
                                           memory='8000', 
                                           stdout=os.path.join(outdir, 'log', f"HR_Analysis_{comb_args['loops']}_{comb_args['channels']}.out"),
                                           stderr=os.path.join(outdir, 'log', f"HR_Analysis_{comb_args['loops']}_{comb_args['channels']}.out"), 
                                           array=well_array)
            clus_cmd = cluster_cmd(cluster, defaults_cluster_kwargs)
            LOGGER.debug('#Cluster command\n' + '\t'.join(clus_cmd))
            job_id, clus_out = run_cluster_and_getid(clus_cmd)
            LOGGER.debug('Output of the cluster command: ' + str(clus_out))
            job_ids_ls.append(job_id)
        else:
            # We do not run the script right away to be able to take into account the number of max processes running at the same time 
            # (see function `run_processes(python_cmd_ls, max_subprocesses)`
            python_cmd_ls.append(bpm_python_cmd) 
            LOGGER.debug('#Python commands\n' + str(bpm_python_cmd))

    #Gather output in the same once every job is finished
    consolidate_python_cmd = prepare_python_cmd(dict(indir= os.path.join(outdir, 'results'), outdir=outdir), os.path.join('src', 'cluster_consolidate.py'))
    if cluster :
        # consolidate_python_cmd = prepare_python_cmd(dict(indir= os.path.join(outdir, 'results'), outdir=outdir), os.path.join('src', 'cluster_consolidate.py'))
        consolidate_cluster_kwargs = dict(script=consolidate_python_cmd,
                                          walltime='2:00:00', 
                                          jobname=f"HR_Consolidate", 
                                          memory='3000',
                                          stdout=os.path.join(outdir,'consolidate.out'), 
                                          stderr=os.path.join(outdir,'consolidate.out'), 
                                          array=None, 
                                          condition_job_ids=job_ids_ls)

        consolidate_cmd = cluster_cmd(cluster, consolidate_cluster_kwargs)
        LOGGER.debug('#Consolidate command\n' + '\t'.join(consolidate_cmd))
        conso_out = subprocess.run(consolidate_cmd,  stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        # LOGGER.debug('#Consolidate command output: ', str(conso_out.stdout.decode('utf-8')))   
                                    
    else:
        LOGGER.info("Running on a single machine")
        # print("Running multifolder mode. Limited console feedback, check logfiles for process status")
        run_processes(python_cmd_ls, max_subprocesses)
        subprocess.Popen(consolidate_python_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        

# TODO: Workaround to import run_algorithm into cluster.py. Maybe solve more elegantly
if __name__ == '__main__':
    # Parse input arguments.
    args = setup.parse_arguments()  
    experiment_id, args = setup.process_arguments(args)

    ################# MULTI FOLDER DETECTION ######################
    # handle type of indir argument Detect subfolders in indir
    print(args.indir)
    if os.path.isdir(args.indir):
        dir_list = io_operations.detect_experiment_directories(args.indir)
    else:
        if os.path.isfile(os.path.dirname(args.indir)):
            with open(os.path.dirname(args.indir), 'r') as f:
                dir_list = [line.replace('\n', '') for line in f.readlines()]
        elif '*' in args.indir:
            dir_list=glob2.glob(args.indir)
        else:
            dir_list = []
        assert len(dir_list) >= 1, "Error: Directory invalid"

    nested_dir = []
    for indir in dir_list:
        nested_dir += list(io_operations.detect_experiment_directories(indir))
    nested_dir = sorted(nested_dir)
    job_index = return_jobindex()
    if job_index != None:
        indir = list(nested_dir)[job_index]
        outdir = os.path.join(args.outdir, os.path.basename(os.path.normpath(indir).replace('/croppedRAWTiff', '')), '')
        os.makedirs(outdir, exist_ok=True)
        os.makedirs(os.path.join(outdir, 'results'), exist_ok=True)
        args.outdir = outdir
        args.indir = indir 
        main(indir, channel_ls=args.channels, loop_ls=args.loops, well_range=args.wells, cluster=args.cluster, outdir=outdir, debug=args.debug, args=args)
    elif len(nested_dir) == 1:
        outdir = os.path.join(args.outdir, os.path.basename(os.path.normpath(list(nested_dir)[0]).replace('/croppedRAWTiff', '')), '')
        os.makedirs(outdir, exist_ok=True)
        os.makedirs(os.path.join(outdir, 'results'), exist_ok=True)
        args.outdir = outdir
        args.indir = list(nested_dir)[0] 
        main(list(nested_dir)[0], channel_ls=args.channels, loop_ls=args.loops, well_range=args.wells, cluster=args.cluster, outdir=outdir, debug=args.debug, args=args)
    else:
        #This means the job has been run without cluster 
        general_outdir = args.outdir
        for indir in nested_dir:
            outdir = os.path.join(general_outdir, os.path.basename(os.path.normpath(indir).replace('/croppedRAWTiff', '')), '')
            os.makedirs(outdir, exist_ok=True)
            os.makedirs(os.path.join(outdir, 'results'), exist_ok=True)
            args.outdir = outdir
            args.indir = indir 
            main(indir, channel_ls=args.channels, loop_ls=args.loops, well_range=args.wells, cluster=args.cluster, outdir=outdir, debug=args.debug, args=args)
   
    
    

