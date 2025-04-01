#!/usr/bin/env python3
############################################################################################################
# Authors:
#   Sebastian Stricker, Uni Heidelberg, sebastian.stricker@stud.uni-heidelberg.de
#   Marcio Ferreira,    EMBL-EBI,       marcio@ebi.ac.uk
# Date: 08/2021
# License: GNU GENERAL PUBLIC LICENSE Version 3
###
# Main cropping program file.
###
############################################################################################################
import gc

import logging
from pathlib import Path
from pathlib import Path
import argparse
import os
import sys 

import pandas as pd

import src.io_operations as io_operations
import src.setup as setup
import src.cropping as cropping
from src.job_utils import return_jobindex

# QC Analysis modules.
from qc_analysis.decision_tree.src import analysis as qc_analysis

# Load config
# Load config
import configparser
curr_dir = Path(__file__).resolve().parent
config_path = curr_dir / 'config.ini'
curr_dir = Path(__file__).resolve().parent
config_path = curr_dir / 'config.ini'
config = configparser.ConfigParser()
config.read(config_path)

################################## GLOBAL VARIABLES ###########################
# LOGGER = logging.getLogger(__name__)
# 
################################## ALGORITHM ##################################
def main(indir, outdir, well_id, loop, channel, args, debug=False):
    LOGGER.info("#######################")
    LOGGER.info("Only cropping, script will not run BPM analyses")
    analysis_id = '_'.join([well_id, loop, channel]) 

    resulting_dict_from_crop = {}
    for well_frame_paths, video_metadata in io_operations.well_video_generator(indir, [channel], [loop]):
        
        # well_nr = int(video_metadata['well_id'][-3:])
        well_nr = video_metadata['well_id']
        if well_id is not None and well_nr != well_id:
            continue
    
        try:
            LOGGER.info("Looking at video - "
                        + "Channel: " + str(video_metadata['channel'])
                        + " Loop: " + str(video_metadata['loop'])
                        + " Well: " + str(video_metadata['well_id'])
                        )

            LOGGER.debug('Number of files corresponding {}'.format(len(well_frame_paths)))
            embryo_size = int(config["CROPPING"]["EMBRYO_SIZE"])
            border_ratio = float(config["CROPPING"]["BORDER_RATIO"])
            
            # we only need the first 5 frames to get position averages
            LOGGER.debug("Loading first 5 frames of video for embryo detection...")
            video8 = io_operations.load_video(well_frame_paths, imread_flag=1, max_frames=5)
            embryo_coordinates = cropping.embryo_detection(video8, embryo_size, border_ratio)
            
            # we need every image as 16 bits to crop based on video8 coordinates
            LOGGER.debug("Loading entire video...")            
            video16 = io_operations.load_video(well_frame_paths, imread_flag=-1)
            cropped_video, resulting_dict_from_crop = cropping.crop_2(video16, embryo_size, embryo_coordinates, resulting_dict_from_crop, video_metadata)
            
            # save cropped images
            LOGGER.debug('Saving cropped video in dir: ' + str(outdir / 'croppedRAWTiff/'))
            io_operations.save_cropped(cropped_video, args, well_frame_paths)
            
            # save panel for crop checking
            LOGGER.debug('Saving panel in dirs: ' + str(outdir / "*_panel.png"))
            io_operations.save_panel(resulting_dict_from_crop, args)
        except:
            LOGGER.error("Problem while cropping for well " + str(video_metadata['well_id'])
                        + " in loop " +
                        str(video_metadata['loop'])
                        + " with channel " + str(video_metadata['channel']))
    LOGGER.info("#######################\n")
            
        

if __name__ == '__main__':
    LOGGER = logging.getLogger(__name__)
    parser = argparse.ArgumentParser(
        description='Automated heart rate analysis of Medaka embryo videos')
    
    # General analysis arguments
    parser.add_argument('-i', '--indir',
                        help='Input directory',
                        required=True)
    
    parser.add_argument('-o', '--outdir',
                        help='Output directory. Default=indir',
                        default='.',
                        required=False)
    
    parser.add_argument('-w', '--well_id',
                        help='Restrict analysis to one well',
                        default=None,   
                        required=False)
    
    parser.add_argument('-a', '--well_array',
                        help='Restrict analysis to targeted wells in case of array',
                        default='[1-96]',   
                        required=False)    

    parser.add_argument('-l', '--loops',
                        help='Restrict analysis to loop',
                        required=False)

    parser.add_argument('-c', '--channels',
                        help='Restrict analysis to channel',
                        required=False)

    parser.add_argument('-f', '--fps',
                        help='Frames per second',
                        default=0.0,
                        required=False,
                        type=float)
    
    parser.add_argument('-s', '--embryo_size', 
                        help='radius of embryo in pixels',
                        default=300,
                        required=False, 
                        type=int)

    parser.add_argument('--cluster',
                        type=str, 
                        choices=['lsf', 'slurm', False], 
                        default=False,
                        help='Run analysis on a cluster',
                        required=False)
    parser.add_argument('--email',
                        action="store_true",
                        help='Receive email for cluster notification',
                        required=False)

    parser.add_argument('-m', '--maxjobs',
                        action="store",
                        help='maxjobs on the cluster',
                        required=False)
    
    parser.add_argument('--debug',
                        action="store_true",
                        help='Additional debug output',
                        required=False)
        
    args = parser.parse_args()    

    # Parse input arguments.
    args.indir = Path(args.indir)
    if args.outdir:
        args.outdir = Path(args.outdir)
    _, args = setup.process_arguments(args, is_cluster_node=False) 
    args.loops =  list(args.loops)[0]
    args.channels =list(args.channels)[0]

    print(args)
    LOGGER.debug(args)
    if args.well_id:
        analysis_id = '_'.join([args.well_id, args.loops, args.channels])
        setup.config_logger(os.path.join(args.outdir, 'log'), ("logfile_crop_{}.log".format(analysis_id)), args.debug)
        main(indir=Path(args.indir), outdir=Path(args.outdir), loop=args.loops, channel=args.channels, well_id=args.well_id, debug=args.debug, args=args)
        sys.exit(0)        
    elif args.well_array:
        range_wells = args.well_array[1:-1].split('-')        
        well_ls = ['WE000' + str(n) if n > 9 else 'WE0000' + str(n) for n in range(int(range_wells[0]), int(range_wells[1])+1)]
    else:
        raise 'Need at least one well to run on'
    
    if args.cluster:
        job_index = return_jobindex()
    else:
        job_index = None
    
    #Run the jobs according to potential arrays
    if job_index != None:
        well_id = well_ls[job_index -1]
        setup.config_logger(os.path.join(args.outdir, 'log'), "logfile_crop_{}.log".format(analysis_id), args.debug)
        main(indir=Path(args.indir), outdir=Path(args.outdir), loop=args.loops, channel=args.channels, well_id=well_id, debug=args.debug, args=args)
    
    elif len(well_ls) == 1:
        setup.config_logger(os.path.join(args.outdir, 'log'), "logfile_crop_{}.log".format(analysis_id), args.debug)
        main(indir=Path(args.indir), outdir=Path(args.outdir), loop=args.loops, channel=args.channels, well_id=well_ls[0], debug=args.debug, args=args)
    else:
        #FIXME: Find a better way to run several wells at a time, multiprocesses ?
        for well_id in well_ls: 
            setup.config_logger(os.path.join(args.outdir, 'log'), "logfile_crop_{}.log".format(analysis_id), args.debug)
            try:
                main(indir=Path(args.indir), outdir=Path(args.outdir), loop=args.loops, channel=args.channels, well_id=well_id, debug=args.debug, args=args)
            except Exception:
                LOGGER.error('Error with well_id {}.'.format(well_id), exc_info=True)
                pass      
