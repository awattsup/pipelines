import os
from casatasks import *
import casampi
from casatools import table,msmetadata
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.gridspec import GridSpec

from PIL import Image
import matplotlib
matplotlib.use('agg')

import bookkeeping
import config_parser

from config_parser import validate_args as va


import dask.bag as db

def QC(MS):

    # MS = "/scratch/awatts/NGC4383-1-1k/NGC4383-1-1k.N4383.ms
    print(MS)
    print(MS.split('.'))
    
    field = MS.split('.')[-2]
    outdir = os.getcwd()+f"/plots/crosscal/{field}"
    print(outdir)
    if not os.path.isdir(outdir):
        os.mkdir(outdir)
    
    
    msmd = msmetadata()
    tb = table()

    msmd.open(MS)
    Nspw = msmd.nspw()
    Nant = msmd.nantennas()
    AntNames = msmd.antennanames()

    tb.open(MS)
    
    dat = tb.query(f"ANTENNA1 == {0} and DATA_DESC_ID=={0}", columns='UVW,TIME')
    times = dat.getcol("TIME")
    Nvis = len(times)
    uvd = dat.getcol("UVW")
    uvd = np.log10(1+np.sqrt(uvd[0,:]*uvd[0,:] + uvd[1,:]*uvd[1,:]))
    
    
    if Nspw>1:
        freq = np.append(msmd.chanfreqs(spw=0,unit='MHz'), msmd.chanfreqs(spw=Nspw-1,unit='MHz') )
    else:
        freq =msmd.chanfreqs(spw=0,unit='MHz')
        
       
    minfreq = np.nanmin(freq)
    maxfreq = np.nanmax(freq)
    mintime = np.nanmin(times)
    maxtime = np.nanmax(times)    
    minuvd = np.nanmin(uvd)
    maxuvd = np.nanmax(uvd)

    for aa in range(Nant):                          
        
        if aa%4==0:
            # fig = plt.figure()
            fig, ax = plt.subplots(4,4,figsize=(11,15),width_ratios = [1,1,1,0.3])

            rangeMin = np.min([4,Nant-1-aa])
            for ii in range(rangeMin):
                ax[ii,3].set_ylim([0.1,500])
                ax[ii,3].set_yscale('log')
                # ax[ii,3].set_xscale('log')
                ax[ii,3].set_xlim([1,8])
                

                ax[ii,0].tick_params(direction='in')
                ax[ii,1].tick_params(direction='in')
                ax[ii,2].tick_params(direction='in')
                ax[ii,3].tick_params(direction='in')

                ax[ii,0].set_ylabel("Amplitude")
                ax[ii,0].set_ylim([0.1,500])
                ax[ii,1].set_ylim([0.1,500])
                ax[ii,2].set_ylim([0.1,500])
                ax[ii,0].set_yscale('log')
                ax[ii,1].set_yscale('log')
                ax[ii,2].set_yscale('log')

                ax[ii,0].set_xlim([mintime,maxtime])
                ax[ii,1].set_xlim([minfreq,maxfreq])
                ax[ii,2].set_xlim([minuvd,maxuvd])

                ax[ii,1].set_yticklabels([])
                ax[ii,2].set_yticklabels([])
                ax[ii,3].set_yticklabels([])

                if ii != 3:
                    ax[ii,0].set_xticklabels([])
                    ax[ii,1].set_xticklabels([])
                    ax[ii,2].set_xticklabels([])
                    ax[ii,3].set_xticklabels([])
                            
                     
                leg = [Line2D([0],[0],color='White',lw=0,markerfacecolor='White',
                marker='.',markersize=0),
                Line2D([0],[0],color='White',lw=0,markerfacecolor='C0',
                marker='s',markersize=3),
                Line2D([0],[0],color='White',lw=0,markerfacecolor='C1',
                marker='s',markersize=3)]

                names = [AntNames[aa+ii],
                 "Raw",
                 "Flagged"]

                ax[ii,2].legend(leg, names,frameon=False)
                            
            ax[rangeMin-1,0].set_xlabel('time')
            ax[rangeMin-1,1].set_xlabel('Frequency [MHz]')
            ax[rangeMin-1,2].set_xlabel('log(1 + UVd) [m]')
            ax[rangeMin-1,3].set_xlabel('log(1+Nvis)')

            fig.canvas.draw()
            
            
            
        ax1 = ax[aa%4,0]
        ax2 = ax[aa%4,1]
        ax3 = ax[aa%4,2]
        ax4 = ax[aa%4,3]
        
        sc11 = ax1.scatter([5],[5],s=1,c="C0",rasterized=True,zorder=0)
        sc12 = ax1.scatter([5],[5],s=1,c='C1',rasterized=True,zorder=1)
        sc21 = ax2.scatter([5],[5],s=1,c="C0",rasterized=True,zorder=0)
        sc22 = ax2.scatter([5],[5],s=1,c='C1',rasterized=True,zorder=1)
        sc31 = ax3.scatter([5],[5],s=1,c="C0",rasterized=True,zorder=0)
        sc32 = ax3.scatter([5],[5],s=1,c='C1',rasterized=True,zorder=1)
        
        h1 = ax4.plot([1],[1],drawstyle='steps',lw=1,color="C0")
        h2 = ax4.plot([1],[1],drawstyle='steps',lw=1,color='C1')
        

        
        
        amp_all = []
        amp_flagged_all = []
        # freq_all = []
        # time_all = []
        # uvd_all = []
        print(f"Plotting Antenna {aa}")
        print(f"Plotting data from {Nspw} SPWs ")
        for ss in range(Nspw):
            dat = tb.query(f"ANTENNA1 == {aa} and DATA_DESC_ID=={ss}", columns='UVW,DATA,FLAG,TIME')
            data = dat.getcol("DATA")
            flags = dat.getcol("FLAG")
            ncorr,nchan,nrows = data.shape
            
            amp = np.absolute(data).flatten()
            amp_flagged = np.ma.masked_array(np.absolute(data),flags).compressed().flatten()
            
            times = dat.getcol("TIME")
            freq = msmd.chanfreqs(spw=ss,unit='MHz')
            uvd = dat.getcol("UVW")
            uvd = np.log10(1+np.sqrt(uvd[0,:]*uvd[0,:] + uvd[1,:]*uvd[1,:]))
            
            times = np.tile(times,(ncorr,nchan,1)).flatten()
            freq = np.rollaxis(np.tile(freq,(ncorr,nrows,1)),2,1).flatten()
            uvd = np.tile(uvd,(ncorr,nchan,1)).flatten()
            # print(uvw.shape)
            # print(uvd.shape)
            
            sc11.set_offsets(np.vstack([times,amp]).T)
            ax1.draw_artist(sc11)

            sc12.set_offsets(np.vstack([np.ma.masked_array(times,flags).compressed(),amp_flagged]).T)
            ax1.draw_artist(sc12)

            sc21.set_offsets(np.vstack([freq,amp]).T)
            ax2.draw_artist(sc21)

            sc22.set_offsets(np.vstack([np.ma.masked_array(freq,flags).compressed(),amp_flagged]).T)
            ax2.draw_artist(sc22)

            sc31.set_offsets(np.vstack([uvd,amp]).T)
            ax3.draw_artist(sc31)

            sc32.set_offsets(np.vstack([np.ma.masked_array(uvd,flags).compressed(),amp_flagged]).T)
            ax3.draw_artist(sc32)

            
            amp_all.extend(amp)
            amp_flagged_all.extend(amp_flagged)

#             freq_all.extend(np.rollaxis(np.tile(freq,(ncorr,nrows,1)),2,1).flatten())
#             time_all.extend(np.tile(times,(ncorr,nchan,1)).flatten())
#             uvd_all.extend(np.tile(uvd,(ncorr,nchan,1)).flatten())


        #Free up some memory
        del uvd, times, data, flags 
#         print(f"Got data, plotting ({len(np.array(amp_all).flatten())}) points")        
        
        
                            
                            
#       

        print('Points plotted, making histograms')
        
        hist,edges= np.histogram(np.array(amp_all),bins=10**np.linspace(-1,np.log10(500),20))
        print(hist,edges)
        h1[0].set_data(np.log10(1+np.array(hist)),np.array(edges)[1:])
        print(h1,h1[0])
        ax4.draw_artist(h1[0])
        hist,edges= np.histogram(np.array(amp_flagged_all),bins=10**np.linspace(-1,np.log10(500),20))
        h2[0].set_data(np.log10(1+np.array(hist)),np.array(edges)[1:])
        ax4.draw_artist(h2[0])
        
        del amp_all, amp_flagged_all#, freq_all, uvd_all, time_all
      
    
        # fig.canvas.draw()

        print(aa)
        if (aa+1)%4==0  and aa!=0 or aa == Nant-1: #(aa+1)%4==1
        # if aa==0 or aa == Nant-1: #(aa+1)%4==1

            print("All done, saving figure")
            print(outdir)
            save(fig, f"{outdir}/flagging_ant{aa-3}_{aa}.png")
            # plt.clf()
            # plt.close()
            print("Saved, moving on to next antenna chunk")





def chunkData(x,y, chunkSize = int(1e4)):
    """Returns a generator over "n" random xy positions and rgb colors."""
    dataLen = len(x)
    for cc in range(0,dataLen,chunkSize):
        ccmax = np.min([cc+chunkSize,dataLen-1])
        xy = np.vstack([x[cc:ccmax],y[cc:ccmax]]).T #,y10 * np.random.random((chunksize, 2))
        # print(xy)
        yield xy

def save(fig, filename):
    # fig.canvas.draw()
    # fig.canvas.flush_events()
    width, height = map(int, fig.get_size_inches() * fig.get_dpi())
    # fig.canvas.buffer_rgba()
    image = Image.frombytes('RGBA', fig.canvas.get_width_height(),fig.canvas.buffer_rgba())
    # image = np.frombuffer(fig.canvas.buffer_rgba(), dtype='uint8')
    # image = image.reshape(width, height,4)
    # image = np.roll(image, -1, 2)
    image.save(filename)
        
        
if __name__ == "__main__":
    args = config_parser.parse_args()
    print(args)
    taskvals, config = config_parser.parse_config(args['config'])
    keepmms = va(taskvals, 'crosscal', 'keepmms', bool)
    ext = 'mms' if keepmms else 'ms'
    
    visname = va(taskvals, 'data', 'vis', str)
    tmpname = os.path.splitext(os.path.split(visname)[1])[0] 
    # print(tmpname)
   

    
    pldir = os.getcwd()+"/plots"
    ccdir = os.getcwd()+"/plots/crosscal"
    if not os.path.isdir(pldir):
        os.mkdir(pldir)
    if not os.path.isdir(ccdir):
        os.mkdir(ccdir)
        
        
    MSnames = []
    
    # + '_%s.im' % (fname)
    for ftype,field in (config['fields']).items():
        f1 = field.replace("'","")
        MSname = f'{tmpname}.{f1}.{ext}'
        if os.path.isdir(MSname) and MSname not in MSnames:
            MSnames.extend([MSname])
            # print(ftype,type(field),MSname)
            
    print(MSnames)        
    
    for MS in MSnames:
        QC(MS)

            
    
    # fields = bookkeeping.get_field_ids(taskvals['fields'])
    # print(fields)