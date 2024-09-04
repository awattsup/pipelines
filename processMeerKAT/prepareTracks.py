import os
import sys
import glob
from shutil import copytree

import config_parser
from config_parser import validate_args as va
import bookkeeping

from casatasks import *
logfile=casalog.logfile()
casalog.setlogfile('logs/{SLURM_JOB_NAME}-{SLURM_JOB_ID}.casa'.format(**os.environ))
import casampi

from casatools import msmetadata,image
msmd = msmetadata()
ia = image()

import logging
from time import gmtime
logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)


from aux_scripts.concat import check_output, get_infiles
from selfcal_scripts.selfcal_part2 import find_outliers, mask_image


# def concatTracks(visname):

    
#     files = visname.split(',')
#     targetfield = files[0].split('.')[-2]
#     ext = files[0].split('.')[-1]    
    
#     trackDirs = ['/'.join(ff.split('/')[:-1]) for ff in files]
#     print(trackDirs)
    
                               
#     outdir = "./"
                               
                               
# #     if ext == 'ms':
              
                               
                               
#     basename = os.getcwd()

#     suffix = f'*.{targetfield}*.ms'
#     files,pattern = get_infiles(trackDirs,suffix)
#     out = f'{basename}/{outdir}/{targetfield}.{ext}'

    
#     if ext == 'ms':
#         MSs = check_output(targetfield,files,pattern,out,job='concat',filetype='MS')
#         if MSs is not None:
#             logger.info('Concatenating MSs with following command:')
#             logger.info('concat(vis={0}, concatvis={1})'.format(MSs,out))
#             concat(vis=MSs, concatvis=out)
#             if fname == fields.targetfield.split(',')[0]:
#                 newvis = out

#             if not os.path.exists(out):
#                 logger.error("Output MS '{0}' attempted to write but was not written.".format(out))
#     if ext == 'mms':
        
#         MMSs = check_output(fname,files,pattern,out,job='virtualconcat',filetype='MMS')
#         if MMSs is not None:
#             MMSs.sort(key=sortbySPW)
#             logger.info('Concatenating MMSs with following command:')
#             logger.info('virtualconcat(vis={0}, concatvis={1})'.format(MMSs,out))
#             virtualconcat(vis=MMSs, concatvis=out)
#             if fname == fields.targetfield.split(',')[0]:
#                 newvis = out

#             if not os.path.exists(out):
#                 logger.error("Output MMS '{0}' attempted to write but was not written.".format(out))
    
    
#     return newvis



def deconvolveMSs(vis, refant, dopol, nloops, loop, cell, robust, imsize, wprojplanes, niter, threshold, uvrange, nterms,
                  gridder, deconvolver, solint, calmode, discard_nloops, gaintype, outlier_threshold, outlier_radius, flag, atrous_do):
    
    imbase,imagename,outimage,pixmask,rmsfile,caltable,prev_caltables,threshold,outlierfile,cfcache,_,_,_,_ = bookkeeping.get_selfcal_args(vis,loop,nloops,nterms,deconvolver,discard_nloops,calmode,outlier_threshold,outlier_radius,threshold,step='tclean')
    
    
    # calcpsf = True
    
    # print(vis)
        
#     imagename = f"{os.getcwd().split('/')[-1]}_im_0"
#     outlierfile = ''
#     pixmask = ''
    
    vis = vis.split(',')

    #SC     
    tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
            imsize=imsize[loop], cell=cell[loop], stokes='I', gridder=gridder[loop],
            wprojplanes = wprojplanes[loop], deconvolver = deconvolver[loop], restoration=True,
            weighting='briggs', robust = robust[loop], niter=niter[loop], outlierfile=outlierfile,
            threshold=threshold[loop], nterms=nterms[loop], calcpsf=calcpsf, # cfcache = cfcache,
            pblimit=-1, mask=pixmask, parallel = True)
    
#SI
#     tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
#             imsize=imsize, cell=cell, stokes=stokes, gridder=gridder, specmode=specmode,
#             wprojplanes = wprojplanes, deconvolver = deconvolver, restoration=True,
#             weighting='briggs', robust = robust, niter=niter, scales=multiscale,
#             restfreq=restfreq, uvtaper = uvtaper, spw=spw,
#             threshold=threshold, nterms=nterms, calcpsf=True, mask=mask, outlierfile=outlierfile,
#             pbcor=False, pblimit=-1, restoringbeam=restoringbeam, parallel = parallel)


 
def makeMask(params):
    
    method = params.pop('method')[0]
    if method == 'default':
        rmsmap,outlierfile = find_outliers(**params,step='bdsf')
        params.pop('atrous_do')
        pixmask = mask_image(**params)
        
     
    #for adding in Sriram's SoFiA-based method here
    
    return rmsmap, outlierfile, pixmask
    
    
    
if __name__ == '__main__':

    args,params = bookkeeping.get_selfcal_params()    
    ##blind deconvolution
    # deconvolveTracks(**params)
    
    #make mask for imaging    
    rmsmap, outlierfile, pixmask = makeMask(params)
    
    
    loop = params['loop']
    
    #update config file for science imaging
    if config_parser.has_section(args['config'], 'image'):
        if config_parser.get_key(args['config'], 'image', 'specmode') != 'cube':
            # Don't copy over continuum mask for cube-mode science imaging; allow for 3D mask (e.g. SoFiA)
            config_parser.overwrite_config(args['config'], conf_dict={'mask' : "'{0}'".format(pixmask)}, conf_sec='image')
        config_parser.overwrite_config(args['config'], conf_dict={'rmsmap' : "'{0}'".format(rmsmap)}, conf_sec='image')
        config_parser.overwrite_config(args['config'], conf_dict={'outlierfile' : "'{0}'".format(outlierfile)}, conf_sec='image')
    # config_parser.overwrite_config(args['config'], conf_dict={'loop' : loop},  conf_sec='selfcal')

    bookkeeping.rename_logs(logfile)