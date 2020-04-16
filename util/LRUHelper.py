from collections import OrderedDict

# a constant limiting the max number of views for each user
VIEW_LIMIT = 2
LRU_view_list = OrderedDict()

#get view according to the view name
def LRU_get(name):
    if name in LRU_view_list:
        value = LRU_view_list.pop(name)
        LRU_view_list[name] = value
    else:
        value = None
    return value


def LRU_set(name, view):
    if name in LRU_view_list:
        value = LRU_view_list.pop(name)
        LRU_view_list[name] = value
    else:
        if len(LRU_view_list) == VIEW_LIMIT:
            LRU_view_list.popitem(last=False)  # pop出第一个item
            LRU_view_list[name] = view
        else:
            LRU_view_list[name] = view


def LRU_reset(name, view):
    LRU_view_list = OrderedDict()
