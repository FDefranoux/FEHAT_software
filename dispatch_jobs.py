import glob
import os
import src.setup as setup
from src.job_utils import *
import src.io_operations as io_operations
import subprocess
import logging
import re
from pathlib import Path

import configparser
curr_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(curr_dir, 'config.ini')
config = configparser.ConfigParser()
config.read(config_path)

MAIN_DIRECTORY = os.path.abspath(os.path.dirname(__file__))
LOGGER = logging.getLogger(__name__)

#TODO: What to do with the DEBUG, MAXJOB arguments?
# if debug : change stdout stderr to a specific file

def main(indir, channel_ls=[], loop_ls=[], well_range='', mode='', cluster=None, outdir='./outdir', debug=False, args={}):

    if mode == 'crop':
        script_python = 'medaka_crop.py'
        memory_job = str(config['DEFAULT']['MEM_CROP'])
    elif mode == 'bpm':
        script_python = 'medaka_bpm.py'
        memory_job = str(config['DEFAULT']['MEM_BPM'])
        
    #TODO: If both?
    
    experiment_name = os.path.basename(os.path.normpath(indir))
    experiment_id = '_'.join(experiment_name.split('/')[0:2])
    setup.config_logger(os.path.join(outdir, 'log'), ("logfile_dispatch_" + experiment_id + ".log"), debug)
    LOGGER.info("##### Job dispatching #####")
    LOGGER.debug('Input directory ' + str(indir))
    LOGGER.debug('Output directory ' + str(outdir))

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
    
    
    if well_range:
        well_range_ls = re.findall('\d{1,2}', well_range)
        if len(well_range_ls) == 2:
            well_range_ls = ['WE000' + str(n) if len(str(n)) == 2 else 'WE0000' + str(n) for n in range(int(well_range_ls[0]), int(well_range_ls[1])+1, 1)]
    
    if set(well_range_ls) != set(wells):
        int_wells = [int(w.replace('WE', '')) for w in set(wells).intersection(well_range_ls)]
        well_array = str([min(int_wells), max(int_wells)]).replace(', ', '-')
    else:
        well_array = well_range

    # assert (len(loops) > 0) or (len(channels) > 0), "No loops or channels were found!"
    combination = [dict(channels=c, loops=p) for c in channels for p in loops]
    LOGGER.debug('The list of combination are: ')
    LOGGER.debug('\n'.join([str(c) for c in combination]))

    
    ################# PREPARE THE RUNS ######################
    python_cmd_ls = []
    job_ids_ls = []
    for comb in combination:
        comb_args =vars(args)
        comb_args.update(comb)
        comb_args['well_array'] = well_array
        comb_args.pop('wells')
        
        #TODO: Add the option to run crop_jobs!
        bpm_python_cmd = prepare_python_cmd(comb_args, script_python)
        if cluster :
            defaults_cluster_kwargs = dict(script=bpm_python_cmd,
                                           walltime='24:00:00', 
                                           jobname="HR_{}_{}".format(comb_args['loops'], comb_args['channels']),
                                           memory=memory_job, 
                                           stdout=os.path.join(outdir, 'log', f"HR_Analysis_{comb_args['loops']}_{comb_args['channels']}.out"),
                                           stderr=os.path.join(outdir, 'log', f"HR_Analysis_{comb_args['loops']}_{comb_args['channels']}.out"), 
                                           array=well_array)
            clus_cmd = cluster_cmd(cluster, defaults_cluster_kwargs)
            LOGGER.debug('Cluster command' + '\t'.join(clus_cmd))
            job_id, clus_out = run_cluster_and_getid(clus_cmd)
            LOGGER.debug('Output of the cluster command: ' + str(clus_out))
            job_ids_ls.append(job_id)
        else:
            # We do not run the script right away to be able to take into account the number of max processes running at the same time 
            # (see function `run_processes(python_cmd_ls, max_subprocesses)`
            python_cmd_ls.append(bpm_python_cmd) 
            LOGGER.debug('Python commands' + str(bpm_python_cmd))

    if (cluster != True):
        LOGGER.info("Running on a single machine the {} processes".format(len(python_cmd_ls)))
        LOGGER.debug(python_cmd_ls[:5])
        # print("Running multifolder mode. Limited console feedback, check logfiles for process status")
        run_processes(python_cmd_ls, max_subprocesses, log=sys.stdout)


    ## CONSOLIDATION ##
    #Gather output in the same once every job is finished
    consolidate_python_cmd = prepare_python_cmd(dict(indir=outdir, outdir=outdir, debug=debug), os.path.join('src', 'cluster_consolidate.py'))
    if (cluster == True) and (mode != 'crop'):
        # consolidate_python_cmd = prepare_python_cmd(dict(indir= os.path.join(outdir, 'results'), outdir=outdir), os.path.join('src', 'cluster_consolidate.py'))
        consolidate_cluster_kwargs = dict(script=consolidate_python_cmd,
                                          walltime='24:00:00', 
                                          jobname=f"HR_Consolidate", 
                                          memory='3000',
                                          stdout=os.path.join(outdir,'consolidate.out'), 
                                          stderr=os.path.join(outdir,'consolidate.out'), 
                                          array=None, 
                                          condition_job_ids=job_ids_ls)

        consolidate_cmd = cluster_cmd(cluster, consolidate_cluster_kwargs)
        LOGGER.debug('Consolidate command' + '\t'.join(consolidate_cmd))
        conso_out = subprocess.run(consolidate_cmd,  stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        LOGGER.debug('Consolidate command output: \n' + str(conso_out.stdout.decode('utf-8')))   
                                    
    elif (cluster != True) and (mode != 'crop'):
        LOGGER.info("Running on a single machine the {} processes".format(len(python_cmd_ls)))
        LOGGER.debug(python_cmd_ls[:5])
        # print("Running multifolder mode. Limited console feedback, check logfiles for process status")
        run_processes(python_cmd_ls, max_subprocesses, log=sys.stdout)
        if mode != 'crop':
            LOGGER.debug('#Consolidate command' + '\t'.join(consolidate_python_cmd))
            conso_out = subprocess.run(consolidate_python_cmd,  stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            LOGGER.debug('Consolidate command output: ' + str(conso_out.stdout.decode('utf-8')))   
    
    elif (cluster != True) and (mode == 'crop'): 
        cropped_files = os.listdir(str(outdir + 'croppedRAWTiff/'))
        LOGGER.info("Cropped, no need for consolidation. Here are the number of cropped files: {}".format(len(cropped_files)))

# TODO: Workaround to import run_algorithm into cluster.py. Maybe solve more elegantly
if __name__ == '__main__':
    # Parse input arguments.
    args = setup.parse_arguments()  
    # experiment_id, args = setup.process_arguments(args)
    
    if args.crop:
        mode = 'crop'
    else:
        mode ='bpm'

    ################# MULTI FOLDER DETECTION ######################
    # handle type of indir argument Detect subfolders in indir
    print(str(args.indir), args.indir)
    if os.path.isdir(args.indir):
        dir_list = io_operations.detect_experiment_directories(args.indir)
    else:
        if os.path.isfile(str(args.indir)):
            with open(str(args.indir), 'r') as f:
                dir_list = [line.replace('\n', '') for line in f.readlines()]
        elif '*' in str(args.indir):
            dir_list=glob.glob(str(args.indir))
        else:
            dir_list = []
        assert len(dir_list) >= 1, "Error: Directory invalid"

    nested_dir = []
    for indir in dir_list:
        nested_dir += list(io_operations.detect_experiment_directories(Path(indir)))
    nested_dir = sorted(nested_dir)
    job_index = return_jobindex()
    if job_index != None:
        indir = list(nested_dir)[job_index]
        outdir = os.path.join(args.outdir, os.path.basename(os.path.normpath(str(indir)).replace('/croppedRAWTiff', '')), '')
        os.makedirs(outdir, exist_ok=True)
        os.makedirs(os.path.join(str(outdir), 'results'), exist_ok=True)
        args.outdir = outdir
        args.indir = indir 
        main(indir, channel_ls=args.channels, loop_ls=args.loops, well_range=args.wells, mode=mode, cluster=args.cluster, outdir=outdir, debug=args.debug, args=args)
    elif len(nested_dir) == 1:
        outdir = os.path.join(args.outdir, os.path.basename(os.path.normpath(list(nested_dir)[0]).replace('/croppedRAWTiff', '')), '')
        os.makedirs(outdir, exist_ok=True)
        os.makedirs(os.path.join(outdir, 'results'), exist_ok=True)
        args.outdir = outdir
        args.indir = list(nested_dir)[0] 
        main(list(nested_dir)[0], channel_ls=args.channels, loop_ls=args.loops, well_range=args.wells, mode=mode, cluster=args.cluster, outdir=outdir, debug=args.debug, args=args)
    else:
        #This means the job has been run without cluster 
        general_outdir = args.outdir
        for indir in nested_dir:
            outdir = os.path.join(general_outdir, os.path.basename(os.path.normpath(indir).replace('/croppedRAWTiff', '')), '')
            os.makedirs(outdir, exist_ok=True)
            os.makedirs(os.path.join(outdir, 'results'), exist_ok=True)
            args.outdir = outdir
            args.indir = indir 
            main(indir=indir, channel_ls=args.channels, loop_ls=args.loops, well_range=args.wells, mode=mode, cluster=args.cluster, outdir=outdir, debug=args.debug, args=args)

