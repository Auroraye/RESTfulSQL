# Least Frequently Used (LFU)
from util import *
# a constant limiting the max number of views for each user
VIEW_LIMIT = 10
LFU_view_list = {}

def LFU_increment(view):
    if view in LFU_view_list:
        LFU_view_list[view] = LFU_view_list[view] + 1
    else:
        if len(LFU_view_list) == VIEW_LIMIT:
            LFU_envict()
            LFU_view_list[view] = 1
        else:
            LFU_view_list[view] = 1


def LFU_envict():
    least_key = None
    least_count = float('inf')
    for key, value in LFU_view_list.items():
        if value < least_count:
            least_key = key
    del LFU_view_list[least_key]


def LFU_reset():
	LFU_view_list = {}


