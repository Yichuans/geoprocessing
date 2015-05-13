import os

from PIL import Image
from Yichuan10 import time_tracker

OUTPUT_LENGTH = 300
ENHANCE_CONTRAST = 1.4 #increase contrast
JPG_QUALITY = 30 # export quality to reduce size

@time_tracker
def process_image(image_path, output_path):
    im_obj = Image.open(image_path)

    # get square crop
    crop_im_obj = crop_square(im_obj)

    # resize
    w, h = im_obj.size
    min_size = min(w, h)

    if min_size <= OUTPUT_LENGTH:
        # no resize needed
        new_im_obj = crop_im_obj

    else:
        # resize required
        resize_im_obj = resize_image(crop_im_obj, OUTPUT_LENGTH)
        new_im_obj = resize_im_obj

    # enhance contrast a bit
    new_im_obj = enhance_contrast_image(new_im_obj)

    # output
    new_im_obj.save(output_path, quality=JPG_QUALITY)


def crop_square(im_obj):
    # crop to the shorter dimension

    w, h = im_obj.size
    min_size = min(w, h)

    if w == min_size:
        # x is the shorter dimension
        x = get_crop_coordinates(min_size, min_size)
        y = get_crop_coordinates(min_size, h)

    else:
        # y is the shorter dimension
        x = get_crop_coordinates(min_size, w)
        y = get_crop_coordinates(min_size, min_size)

    # (left, upper, right, lower)
    crop_box = (x[0], y[0], x[1], y[1])
    new_im_obj = im_obj.crop(crop_box)

    return new_im_obj

def resize_image(im_obj, require_length):
    # resize to OUTPUT_LENGTH
    resize_box = (require_length, require_length)
    ##  debug
    # print(resize_box)

    new_im_obj = im_obj.resize(resize_box)

    return new_im_obj

def enhance_contrast_image(im_obj):
    from PIL import ImageEnhance

    enh = ImageEnhance.Contrast(im_obj)
    new_im_obj = enh.enhance(ENHANCE_CONTRAST)
    return new_im_obj


def get_crop_coordinates(required_length, total_length):
    """return coordinates for cropping in one direction"""
    from math import ceil, floor
    
    if required_length >= total_length:
        return (0, total_length)

    else:
        # need integers
        d = (total_length - required_length)/2.0
        return (int(floor(d)), int(ceil(required_length+d)))


# ==========================
# test suite

def test_run():
    os.chdir(r"D:\unesco_pic\1167")

    for each in os.listdir(os.getcwd()):
        # create output folder if needed
        if not os.path.exists('output'):
            os.mkdir('output')

        if each.endswith('.jpg'):
            inputsrc = each
            outputsrc = os.getcwd() + os.sep + 'output' + os.sep + inputsrc
            process_image(inputsrc, outputsrc)


# test_run()

def run_all():
    export_folder = r"D:\web2py_2014\applications\wh_app\views\experiment\unesco_pic_resize"
    if not os.path.exists(export_folder):
        os.mkdir(export_folder)

    input_folder = r"D:\unesco_pic"
    for folder, subfolder, pics in os.walk(input_folder):
        for pic in pics:
            pic_path = folder + os.sep + pic

            if pic_path.endswith('.jpg'):
                inputsrc = pic_path
                outputsrc = export_folder + os.sep + pic
                process_image(inputsrc, outputsrc)


run_all()


