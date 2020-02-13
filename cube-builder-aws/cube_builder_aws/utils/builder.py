import numpy
import datetime
import rasterio
from datetime import timedelta
from numpngw import write_png
from scipy import ndimage as ndi

#############################
def days_in_month(date):
	year = int(date.split('-')[0])
	month = int(date.split('-')[1])
	nday = int(date.split('-')[2])
	if month == 12:
		nmonth = 1
		nyear = year +1
	else:
		nmonth = month + 1
		nyear = year
	ndate = '{0:4d}-{1:02d}-{2:02d}'.format(nyear,nmonth,nday)
	td = numpy.datetime64(ndate) - numpy.datetime64(date)
	return td


#############################
def decode_periods(start_date, end_date, temporalschema, timestep):
	requestedperiods = {}

	tdtimestep = datetime.timedelta(days=timestep)
	stepsperperiod = int(round(365./timestep))

	if temporalschema is None:
		periodkey = start_date + '_' + start_date + '_' + end_date
		requestedperiod = []
		requestedperiod.append(periodkey)
		requestedperiods[start_date] = requestedperiod
		return requestedperiods

	if temporalschema == 'M':
		start_date = numpy.datetime64(start_date)
		end_date = numpy.datetime64(end_date)
		requestedperiod = []
		while start_date <= end_date:
			next_date = start_date + days_in_month(str(start_date))
			periodkey = str(start_date)[:10] + '_' + str(start_date)[:10] + '_' + str(next_date - numpy.timedelta64(1, 'D'))[:10]
			requestedperiod.append(periodkey)
			requestedperiods[start_date] = requestedperiod
			start_date = next_date
		return requestedperiods

    # Find the exact start_date based on periods that start on yyyy-01-01
	firstyear = start_date.split('-')[0]
	new_start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
	if temporalschema == 'A':
		dbase = datetime.datetime.strptime(firstyear+'-01-01', '%Y-%m-%d')
		while dbase < new_start_date:
			dbase += tdtimestep
		if dbase > new_start_date:
			dbase -= tdtimestep
		start_date = dbase.strftime('%Y-%m-%d')
		new_start_date = dbase

    # Find the exact end_date based on periods that start on yyyy-01-01
	lastyear = end_date.split('-')[0]
	new_end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
	if temporalschema == 'A':
		dbase = datetime.datetime.strptime(lastyear+'-12-31', '%Y-%m-%d')
		while dbase > new_end_date:
			dbase -= tdtimestep
		new_end_date = dbase
		if new_end_date == new_start_date:
			new_end_date += tdtimestep - datetime.timedelta(days=1)
		end_date = new_start_date.strftime('%Y-%m-%d')

    # For annual periods
	if temporalschema == 'A':
		dbase = new_start_date
		yearold = dbase.year
		count = 0
		requestedperiod = []
		while dbase < new_end_date:
			if yearold != dbase.year:
				dbase = datetime.datetime(dbase.year,1,1)
			yearold = dbase.year
			dstart = dbase
			dend = dbase + tdtimestep - datetime.timedelta(days=1)
			dend = min(datetime.datetime(dbase.year,12,31),dend)
			basedate = dbase.strftime('%Y-%m-%d')
			start_date = dstart.strftime('%Y-%m-%d')
			end_date = dend.strftime('%Y-%m-%d')
			periodkey = basedate + '_' + start_date + '_' + end_date
			if count % stepsperperiod == 0:
				count = 0
				requestedperiod = []
				requestedperiods[basedate] = requestedperiod
			requestedperiod.append(periodkey)
			count += 1
			dbase += tdtimestep
		if len(requestedperiods) == 0 and count > 0:
			requestedperiods[basedate].append(requestedperiod)
	else:
		yeari = start_date.year
		yearf = new_end_date.year
		monthi = start_date.month
		monthf = new_end_date.month
		dayi = start_date.day
		dayf = new_end_date.day
		for year in range(yeari,yearf+1):
			dbase = datetime.datetime(year,monthi,dayi)
			if monthi <= monthf:
				dbasen = datetime.datetime(year,monthf,dayf)
			else:
				dbasen = datetime.datetime(year+1,monthf,dayf)
			while dbase < dbasen:
				dstart = dbase
				dend = dbase + tdtimestep - datetime.timedelta(days=1)
				basedate = dbase.strftime('%Y-%m-%d')
				start_date = dstart.strftime('%Y-%m-%d')
				end_date = dend.strftime('%Y-%m-%d')
				periodkey = basedate + '_' + start_date + '_' + end_date
				requestedperiod = []
				requestedperiods[basedate] = requestedperiod
				requestedperiods[basedate].append(periodkey)
				dbase += tdtimestep
	return requestedperiods


#############################
def encode_key(activity, keylist):
	dynamoKey = ''
	for key in keylist:
		dynamoKey += activity[key]
	return dynamoKey


############################
def getMaskStats(mask):
	totpix   = mask.size
	clearpix = numpy.count_nonzero(mask==1)
	cloudpix = numpy.count_nonzero(mask==2)
	imagearea = clearpix+cloudpix
	clearratio = 0
	cloudratio = 100
	if imagearea != 0:
		clearratio = round(100.*clearpix/imagearea,1)
		cloudratio = round(100.*cloudpix/imagearea,1)
	efficacy = round(100.*clearpix/totpix,2)
	return (cloudratio,clearratio,efficacy)


############################
def getMask(raster, dataset):
    # Output Cloud Mask codes
    # 0 - fill
    # 1 - clear data
    # 0 - cloud
	if 'LC8SR' in dataset:
		# Input pixel_qa codes
		fill = 1 				# warped images have 0 as fill area
		terrain = 2					# 0000 0000 0000 0010
		radsat = 4+8				# 0000 0000 0000 1100
		cloud = 16+32+64			# 0000 0000 0110 0000
		shadow = 128+256			# 0000 0001 1000 0000
		snowice = 512+1024			# 0000 0110 0000 0000
		cirrus = 2048+4096			# 0001 1000 0000 0000

		# Start with a zeroed image imagearea
		imagearea = numpy.zeros(raster.shape, dtype=numpy.bool_)
		# Mark with True the pixels that contain valid data
		imagearea = imagearea + raster > fill
		# Create a notcleararea mask with True where the quality criteria is as follows
		notcleararea = (raster & radsat > 4) + \
            (raster & cloud > 64) + \
            (raster & shadow > 256) + \
            (raster & snowice > 512) + \
            (raster & cirrus > 4096)

		strel = numpy.ones((6, 6), dtype=numpy.uint8)
		out = numpy.empty(notcleararea.shape, dtype=numpy.bool)
		ndi.binary_dilation(notcleararea, structure=strel, output=out)
		notcleararea = out

		remove_small_holes(notcleararea, area_threshold=80, connectivity=1, in_place=True)

		# Clear area is the area with valid data and with no Cloud or Snow
		cleararea = imagearea * numpy.invert(notcleararea)
		# Code the output image rastercm as the output codes
		rastercm = (2*notcleararea + cleararea).astype(numpy.uint16)  # .astype(numpy.uint8)

	elif dataset == 'MOD13Q1' or dataset == 'MYD13Q1':
		# MOD13Q1 Pixel Reliability !!!!!!!!!!!!!!!!!!!!
		# Note that 1 was added to this image in downloadModis because of warping
		# Rank/Key Summary QA 		Description
		# -1 		Fill/No Data 	Not Processed
		# 0 		Good Data 		Use with confidence
		# 1 		Marginal data 	Useful, but look at other QA information
		# 2 		Snow/Ice 		Target covered with snow/ice
		# 3 		Cloudy 			Target not visible, covered with cloud
		fill = 0 	# warped images have 0 as fill area
		lut = numpy.array([0, 1, 1, 2, 2], dtype=numpy.uint8)
		rastercm = numpy.take(lut, raster+1).astype(numpy.uint8)

	elif 'S2SR' in dataset:
		# S2 sen2cor - The generated classification map is specified as follows:
		# Label Classification
		#  0		NO_DATA
		#  1		SATURATED_OR_DEFECTIVE
		#  2		DARK_AREA_PIXELS
		#  3		CLOUD_SHADOWS
		#  4		VEGETATION
		#  5		NOT_VEGETATED
		#  6		WATER
		#  7		UNCLASSIFIED
		#  8		CLOUD_MEDIUM_PROBABILITY
		#  9		CLOUD_HIGH_PROBABILITY
		# 10		THIN_CIRRUS
		# 11		SNOW
		# 0 1 2 3 4 5 6 7 8 9 10 11
		lut = numpy.array([0,0,2,2,1,1,1,2,2,2,1, 1],dtype=numpy.uint8)
		rastercm = numpy.take(lut,raster).astype(numpy.uint8)

	elif dataset == 'CB4_AWFI' or dataset == 'CB4_MUX':
		# Key 		Summary QA 		Description
		# 0 		Fill/No Data 	Not Processed
		# 127 		Good Data 		Use with confidence
		# 255 		Cloudy 			Target not visible, covered with cloud
		fill = 0 		# warped images have 0 as fill area
		lut = numpy.zeros(256,dtype=numpy.uint8)
		lut[127] = 1
		lut[255] = 2
		rastercm = numpy.take(lut,raster).astype(numpy.uint8)

	totpix   = rastercm.size
	clearpix = numpy.count_nonzero(rastercm==1)
	cloudpix = numpy.count_nonzero(rastercm==2)
	imagearea = clearpix+cloudpix
	cloudratio = 100
	if imagearea != 0:
		cloudratio = round(100.*cloudpix/imagearea,1)
	efficacy = round(100.*clearpix/totpix,2)

	return rastercm.astype(numpy.uint16), efficacy, cloudratio


############################
def generateQLook(generalSceneId, qlfiles):
    profile = None
    with rasterio.open(qlfiles[0]) as src:
        profile = src.profile

    numlin = 768
    numcol = int(float(profile['width'])/float(profile['height'])*numlin)
    image = numpy.ones((numlin,numcol,len(qlfiles),), dtype=numpy.uint8)
    pngname = '/tmp/{}.png'.format(generalSceneId)

    nb = 0
    for file in qlfiles:
        with rasterio.open(file) as src:
            raster = src.read(1, out_shape=(numlin, numcol))
            # Rescale to 0-255 values
            nodata = raster <= 0
            if raster.min() != 0 or raster.max() != 0:
                raster = raster.astype(numpy.float32)/10000.*255.
                raster[raster>255] = 255
            image[:,:,nb] = raster.astype(numpy.uint8) * numpy.invert(nodata)
            nb += 1

    write_png(pngname, image, transparent=(0, 0, 0))
    return pngname

############################
def remove_small_holes(ar, area_threshold=64, connectivity=1, in_place=False):
    """
	@author (scikit-image)
	- https://github.com/scikit-image/scikit-image/blob/master/skimage/morphology/misc.py
    """

    if in_place:
        out = ar
    else:
        out = ar.copy()

    # Creating the inverse of ar
    if in_place:
        numpy.logical_not(out, out=out)
    else:
        out = numpy.logical_not(out)

    # removing small objects from the inverse of ar
    out = remove_small_objects(out, area_threshold, connectivity, in_place)

    if in_place:
        numpy.logical_not(out, out=out)
    else:
        out = numpy.logical_not(out)

    return out

def remove_small_objects(ar, min_size=64, connectivity=1, in_place=False):
    """
	@author (scikit-image)
	- https://github.com/scikit-image/scikit-image/blob/master/skimage/morphology/misc.py
    """
    # Raising type error if not int or bool
    if in_place:
        out = ar
    else:
        out = ar.copy()

    if min_size == 0:  # shortcut for efficiency
        return out

    if out.dtype == bool:
        selem = ndi.generate_binary_structure(ar.ndim, connectivity)
        ccs = numpy.zeros_like(ar, dtype=numpy.int32)
        ndi.label(ar, selem, output=ccs)
    else:
        ccs = out

    try:
        component_sizes = numpy.bincount(ccs.ravel())
    except ValueError:
        raise ValueError("Negative value labels are not supported. Try "
                         "relabeling the input with `scipy.ndimage.label` or "
                         "`skimage.morphology.label`.")

    too_small = component_sizes < min_size
    too_small_mask = too_small[ccs]
    out[too_small_mask] = 0
    return out
