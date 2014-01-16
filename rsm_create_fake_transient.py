#!/usr/bin/env python

import os, subprocess, optparse, sys, glob
from multiprocessing import Pool
from functools import partial

usage = "usage: python %prog [options]"
description="Insert a fake transient into a NCP dataset."
vers="1.0"

parser = optparse.OptionParser(usage=usage, version="%prog v{0}".format(vers), description=description)
parser.add_option("-b", "--targetid", action="store", type="string", dest="targetid", default="L111112", help="Target id [default: %default]")
parser.add_option("-c", "--calibid", action="store", type="string", dest="calibid", default="L111111", help="Calibrator id [default: %default]")
parser.add_option("-d", "--data", action="store", type="string", dest="datadir", default="/media/RAIDD/lofar_data/RSM/FullScan-2013-02-10/", help="Target id [default: %default]")
parser.add_option("-r", "--procdata", action="store", type="string", dest="calibdir", default="/media/RAIDK/as24v07/RSM_Processing/Zenith_Scan_2013-02-10/POINTING19/", help="Target id [default: %default]")
parser.add_option("-f", "--flux", action="store", type="float", dest="flux", default=15.0, help="Set flux level [default: %default]")
parser.add_option("-t", "--time", action="store", type="int", dest="time", default=11, help="State the duration of the transient source [default: %default]")
parser.add_option("-D", "--diff", action="store", type="int", dest="diff", default=40, help="Number of subbands in each beam [default: %default]")
parser.add_option("-B", "--beams", action="store", type="string", dest="beams", default="4,5", help="Beams to add transient to [default: %default]")
(options, args) = parser.parse_args()

def writemodel(flux):
	sys.stdout.write("Writing Model File...")
	sys.stdout.flush()
	model=open("fake_trans.model", 'w')
	model.write("# (Name, Type, Ra, Dec, I, Q, U, V, ReferenceFrequency='149e6',  SpectralIndex='[0.0]', MajorAxis, MinorAxis, Orientation) = format\n\
\n\
FAKE, POINT, 13:18:18.0, +54.21.55.0, {0}, , , , , [-0.8]".format(flux))
	model.close()
	print "Done!"

def createset(sb,flux,time,dif, cdir, c):
	sbname=sb.split("/")[-1]
	print "Grabbing {0}...".format(sbname)
	# sys.stdout.flush()
	# msname="L43341_SAP003_SB240_uv.MS.NEW_Feb13_1CHNL.TRANS_{0}_{1}Mins.dppp".format(flux, time)
	subprocess.call(["cp", "-r", sb, "."])
	# print "Done!"
	print "Copying respective calibrator instrument table for {0}...".format(sbname)
	# sys.stdout.flush()
	beam=int(sbname.split("SAP")[1][:3])
	sbnum=int(sbname.split("SB")[1][:3])
	calibnum=sbnum-(dif*beam)
	calibsb=os.path.join(cdir, "Calibrators", c, "datasets", "{0}_SAP000_SB{1:03d}_uv.MS.dppp.tmp".format(c,calibnum))
	# subprocess.call("cp -r {0} {1}".format(os.path.join(cdir, "Calibrators", c, "datasets", "*SB{0:03d}*.tmp".format(calibnum), "instrument"), sbname), shell=True)
	subprocess.call("parmexportcal in={0}/instrument/ out={1}.parmdb > parmexportcal_{1}_log.txt 2>&1".format(calibsb, sbname), shell=True)
	# print "parmexportcal in={0}/instrument/ out={1}.parmdb > parmexportcal_{1}_log.txt 2>&1".format(calibsb, sbname)
	print "Done!"
	print "Adding transient using BBS ADD to {0}...".format(sbname)
	# sys.stdout.flush()
	# subprocess.call("calibrate-stand-alone {0} bbs_add.parset fake_trans.model > bbsaddlog_{0}.txt 2>&1".format(sbname), shell=True)
	subprocess.call("calibrate-stand-alone --parmdb {0}.parmdb {0} bbs_add.parset fake_trans.model > bbsaddlog_{0}.txt 2>&1".format(sbname), shell=True)
	print "{0} Done!".format(sbname)
	
	
f=options.flux
t=options.time
d=options.diff

print "Inserting a fake transient of flux {0} Jy at 150 MHz".format(f)
writemodel(f)
target_list=sorted(glob.glob(os.path.join(options.datadir, options.targetid+"_REAL", "*SAP00[{0}]*.dppp".format(options.beams))))
createset_multi=partial(createset, flux=f, time=t, dif=d, cdir=options.calibdir, c=options.calibid)
workers=Pool(processes=12)
workers.map(createset_multi, target_list)
dirname="{0}_{1}Jy".format(options.targetid, f)
os.mkdir(dirname)
os.mkdir("logs")
subprocess.call("mv {0}*.dppp *.parmdb {1}".format(options.targetid, dirname), shell=True)
subprocess.call("mv parmexportcal_* bbsaddlog*.txt logs", shell=True)
subprocess.call("mv logs {0}".format(dirname), shell=True)
print "Finished."
