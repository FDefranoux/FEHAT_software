#!/usr/bin/env python3
############################################################################################################
# Authors:
#   Sebastian Stricker, Uni Heidelberg, sebastian.stricker@stud.uni-heidelberg.de
#   Marcio Ferreira,    EMBL-EBI,       marcio@ebi.ac.uk
# Date: 08/2021
# License: Contact authors
###
# Main program file.
###
############################################################################################################
import gc

import logging
from pathlib import Path
import subprocess
import sys
# import glob2
import cv2
import argparse

import pandas as pd

import src.io_operations as io_operations
import src.setup as setup
import src.segment_heart as segment_heart
import src.cropping as cropping
from src.job_utils import return_jobindex

# QC Analysis modules.
from qc_analysis.decision_tree.src import analysis as qc_analysis

# Load config
import configparser
curr_dir = Path(__file__).resolve().parent
config_path = curr_dir / 'config.ini'
config = configparser.ConfigParser()
config.read(config_path)

################################## GLOBAL VARIABLES ###########################
LOGGER = logging.getLogger(__name__)

################################## ALGORITHM ##################################
def clusterpy(results):
    # Results here is the output of the analyse_directory 
        # write bpm in tmp directory
        out_string = ""
        for col in results.columns:
            value = results[col][0]

            if not pd.isnull(value):
                value = str(value)
            else:
                value = 'NA'

            out_string += f"{col}:{value};"

        out_string = out_string[:-1]

        out_file = os.path.join(tmp_dir, (analysis_id + '.txt'))
        with open(out_file, 'w') as output:
            output.write(out_string)
    
# Analyse a range of wells
def analyse_directory(indir, args, channels, loops, wells=None):
    LOGGER.info("##### Analysis #####")
    LOGGER.info("The analysis for each well can take one to several minutes")
    LOGGER.info("Running....please wait...\n")

    # Results for all wells
    results = pd.DataFrame()

    # Get trained model, if present. 
    trained_tree = io_operations.load_decision_tree()
    
    try:
        resulting_dict_from_crop = {}
        for well_frame_paths, video_metadata in io_operations.well_video_generator(indir, channels, loops):
            
            well_nr = int(video_metadata['well_id'][-3:])
            if wells is not None and well_nr not in wells:
                continue

            # Results of current well
            well_result = {}
            bpm = None
            fps = None
            qc_attributes = {}
            
            try:
                bpm, fps, qc_attributes = analyse_well(well_frame_paths, video_metadata, args, resulting_dict_from_crop)
                LOGGER.info(f"Reported BPM: {str(bpm)}\n")
                
                # Process data.
                # Important to rearrange the qc params in the same order used during training.
                # Easiest way to do that is to convert the qc_attributes to a dataframe and reorder the columns.
                # 'Stop frame' is not used during training.
                if trained_tree and bpm:
                    data = {k: v for k, v in qc_attributes.items() if k not in ["Stop frame"]}
                    data = pd.DataFrame.from_dict(qc_attributes, orient = "index").transpose()[qc_analysis.QC_FEATURES]
                    
                    # Get the qc parameter results evaluated by the decision tree as a dictionary.
                    qc_analysis_results = qc_analysis.evaluate(trained_tree, data)
                    well_result["qc_param_decision"] = qc_analysis_results[0]

                # qc_attributes may help in dev to improve the algorithm, but are unwanted in production.
                if True: #args.debug:
                    well_result.update(qc_attributes)

            except Exception as e:
                LOGGER.exception("Couldn't acquier BPM for well " + str(video_metadata['well_id'])
                                    + " in loop " +
                                    str(video_metadata['loop'])
                                    + " with channel " + str(video_metadata['channel']))
                well_result['error'] = "Error during processing. Check log files"

            finally:
                well_result['well_id']  = video_metadata['well_id']
                well_result['loop']     = video_metadata['loop']
                well_result['channel']  = video_metadata['channel']
                well_result['bpm']      = bpm
                well_result['fps']      = fps
                well_result['version']  = config['DEFAULT']['VERSION']
                        
                results = results.append(well_result, ignore_index=True)

                gc.collect()

    except Exception as e:
        LOGGER.exception("Couldn't finish analysis")
        sys.exit()

    return results

# Run algorithm on a single well
def analyse_well(well_frame_paths, video_metadata, args, resulting_dict_from_crop):
    LOGGER.info("Analysing video - "
                + "Channel: " + str(video_metadata['channel'])
                + " Loop: " + str(video_metadata['loop'])
                + " Well: " + str(video_metadata['well_id'])
                )

    # TODO: I/O is very slow, 1 video ~500mb ~20s locally. Buffer video loading for single machine?
    # Load video
    video_metadata['timestamps'] = io_operations.extract_timestamps(well_frame_paths)

    # This does not overlap with analysis and should therefore be in it's own function
    # Crop and analyse
    if args.crop == True and args.crop_and_save == False:
        LOGGER.info("Cropping images to analyze them, but NOT saving cropped images")
        # We only need 8 bits video as no images will be saved
        video8 = io_operations.load_video(well_frame_paths, imread_flag=1)

        # now calculate position based on first 5 frames 8 bits
        embryo_coordinates = cropping.embryo_detection(video8[0:5])  # get the first 5 frames

        # crop and do not save, just return 8 bits cropped video
        video, resulting_dict_from_crop = cropping.crop_2(
            video8, args, embryo_coordinates, resulting_dict_from_crop, video_metadata)

        video = [cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) for frame in video]

        # save panel for crop checking
        io_operations.save_panel(resulting_dict_from_crop, args)

    elif args.crop_and_save == True:
        LOGGER.info("Cropping images and saving them...")
        # first 5 frames to calculate embryo coordinates
        video8 = io_operations.load_video(well_frame_paths, imread_flag=1, max_frames=5)

        # we need every image as 16 bits to crop based on video8 coordinates
        video16 = io_operations.load_video(well_frame_paths, imread_flag=-1)
        embryo_coordinates = cropping.embryo_detection(video8)
        video_cropped, resulting_dict_from_crop = cropping.crop_2(
            video16, args, embryo_coordinates, resulting_dict_from_crop, video_metadata)  

        # save cropped images
        io_operations.save_cropped(video_cropped, args, well_frame_paths)

        # save panel for crop checking
        io_operations.save_panel(resulting_dict_from_crop, args)

        # now we need every frame in 8bits to run bpm
        video = io_operations.load_video(well_frame_paths, imread_flag=0)
    else:
        video = io_operations.load_video(well_frame_paths, imread_flag=0)

    bpm, fps, qc_attributes = segment_heart.run(video, vars(args), video_metadata)

    return bpm, fps, qc_attributes

def main(indir, outdir, well_id, loop, channel, args, debug=False):
    ################################## STARTUP SETUP ##################################
    LOGGER.debug('Input directory ' + indir)
    LOGGER.debug('Output directory ' + outdir)
    analysis_id = '_'.join([well_id, loop, channel])
    
    setup.config_logger(os.path.join(outdir, 'log'), ("logfile_" + analysis_id + ".log"), debug)
    LOGGER.info(f"##### MedakaBPM {well_id}--{loop}--{channel} #####")
    LOGGER.info("Program started with the following arguments: " + '\t'.join([indir, outdir, well_id, loop, channel]))
    ################################## MAIN PROGRAM START ##################################

    pattern = os.path.join(indir, f'*--{loop}--{channel}--*{well_id}.tif')
    LOGGER.debug(pattern) 
    nr_files_for_analysis = glob2.glob(pattern) + glob2.glob(pattern+'f')
    if len(nr_files_for_analysis) < 0:
        LOGGER.error("Did not found any files corresponding")
        raise Exception('No File Found') 
    else:
        LOGGER.info(f'Found {len(nr_files_for_analysis)} for the analysis')
        LOGGER.debug('List of files included in analysis:\n' + '\n'.join(nr_files_for_analysis))

        well_nr = int(well_id[-3:])
        results = analyse_directory(indir, args, [channel], [loop], wells=[well_nr])

        ################################## OUTPUT ##################################
        LOGGER.info("# Finished analysis")

        # nr_of_results = len(results)
        # if (nr_of_videos != nr_of_results):
        #     LOGGER.warning("Logic fault. Number of results (" + str(nr_of_results) +
        #                     ") doesn't match number of videos detected (" + str(nr_of_videos) + ")")
        try:
            io_operations.write_to_spreadsheet(os.path.join(outdir, 'results'), results, analysis_id) 
        except:
            LOGGER.debug('Problem saving here: '+ os.path.join(outdir, 'results') + os.path.isdir(os.path.join(outdir, 'results')))
            LOGGER.warning('Error while saving')

# TODO: Workaround to import run_algorithm into cluster.py. Maybe solve more elegantly
if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Automated heart rate analysis of Medaka embryo videos')
    # General analysis arguments
    parser.add_argument('-i', '--indir',     action="store",         dest='indir',
                        help='Input directory',                                 default=False,      required=True)
    parser.add_argument('-o', '--outdir',    action="store",         dest='outdir',
                        help='Output directory. Default=indir',                 default=False,      required=False)
    parser.add_argument('-w', '--wells',     action="store",         dest='wells',
                        help='Restrict analysis to wells',                      default='[1-96]',   required=False)
    parser.add_argument('-l', '--loops',     action="store",         dest='loops',
                        help='Restrict analysis to loop',                       default=None,       required=False)
    parser.add_argument('-c', '--channels',  action="store",         dest='channels',
                        help='Restrict analysis to channel',                    default=None,       required=False)
    parser.add_argument('-f', '--fps',       action="store",         dest='fps',
                        help='Frames per second',                               default=0.0,        required=False,   type=float)
    parser.add_argument('-s', '--embryo_size', action="store",         dest='embryo_size',
                        help='radius of embryo in pixels',                  default=300,        required=False, type=int)
    parser.add_argument('--cluster',        action="store",    dest='cluster', type=str, choices=['lsf', 'slurm', False], default=False,
                        help='Run analysis on a cluster',                       required=False)
    parser.add_argument('--debug',          action="store_true",    dest='debug',
                        help='Additional debug output',                          required=False)
        
    args = parser.parse_args()


    # Parse input arguments.
    dispatch_args = setup.parse_arguments()
    LOGGER.info('OUTDIR MEDAKA '+ args.outdir + '\t' + dispatch_args.outdir)
    range_wells = dispatch_args.wells[1:-1].split('-')
    well_ls = ['WE000' + str(n) if n > 9 else 'WE0000' + str(n) for n in range(int(range_wells[0]), int(range_wells[1])+1)]
    if args.cluster:
        job_index = return_jobindex()
    else:
        job_index = None
    print(job_index)
    if job_index != None:
        well_id = well_ls[job_index -1]
        main(indir=args.indir, outdir=args.outdir, well_id=well_id, loop=args.loops, channel=args.channels, debug=args.debug, args=dispatch_args)
    elif len(well_ls) == 1:
        main(indir=args.indir, outdir=args.outdir, well_id=well_ls[0], loop=args.loops, channel=args.channels, debug=args.debug, args=dispatch_args)
    else:
        #FIXME: Find a better way to run several wells at a time, multiprocesses ?
        for well_id in well_ls: 
            main(indir=args.indir, outdir=args.outdir, well_id=well_id, loop=args.loops, channel=args.channels, debug=args.debug, args=dispatch_args)