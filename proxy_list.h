
#ifndef __LIBCEPHFS_PROXY_LIST_H__
#define __LIBCEPHFS_PROXY_LIST_H__

#include "proxy.h"

#define LIST_INIT(_list) { _list, _list }

#define list_entry(_ptr, _type, _field) container_of(_ptr, _type, _field)

#define list_first_entry(_list, _type, _field) \
    list_entry((_list)->next, _type, _field)

#define list_next_entry(_ptr, _field) \
    list_first_entry(&_ptr->_field, __typeof(*_ptr), _field)

#define list_for_each_entry(_ptr, _list, _field) \
    for (_ptr = list_first_entry(_list, __typeof(*_ptr), _field); \
         &_ptr->_field != _list; \
         _ptr = list_next_entry(_ptr, _field))

static inline void
list_init(list_t *list)
{
    list->next = list;
    list->prev = list;
}

static inline bool
list_empty(list_t *list)
{
    return list->next == list;
}

static inline void
list_add(list_t *item, list_t *list)
{
    item->next = list->next;
    item->prev = list;
    list->next->prev = item;
    list->next = item;
}

static inline void
list_add_tail(list_t *item, list_t *list)
{
    item->next = list;
    item->prev = list->prev;
    list->prev->next = item;
    list->prev = item;
}

static inline void
list_del(list_t *list)
{
    list->next->prev = list->prev;
    list->prev->next = list->next;
}

static inline void
list_del_init(list_t *list)
{
    list_del(list);
    list_init(list);
}

static inline void
list_move(list_t *item, list_t *list)
{
    list_del(item);
    list_add(item, list);
}

static inline void
list_move_tail(list_t *item, list_t *list)
{
    list_del(item);
    list_add_tail(item, list);
}

#endif
