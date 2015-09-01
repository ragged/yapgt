#!/usr/bin/env python
import re
import sys
import urwid
import time
import argparse

import psycopg2
import psycopg2.extras

from pprint import pprint



#Some Options
parser = argparse.ArgumentParser(description="YAPGT - Yet Another PostgreSQL Tool")
parser.add_argument("-H", "--host", default="localhost", dest="host", help="Host to connect to (not tested yet)")
parser.add_argument("-p", "--port", default="5432", dest="port", help="Which port to connect to")
parser.add_argument("-d", "--database", dest="database", help="From which database should the statistics be collected")
parser.add_argument("-u", "--user", default="postgres", dest="user", help="Which user shall connect")
parser.add_argument("-w", "--password", dest="password", help="The password of the DB")

args = parser.parse_args()

print args

DEBUG = False

palette = [
    ('top', 'light gray', 'black'),
    ('header', 'black', 'dark green'),
    ('header_focus', 'black', 'light green'),
    ('body', 'black', 'light gray'),
    ('footer', 'white', 'dark gray'),
    ('main', 'light gray', 'dark red'),
    ('bottom', 'light gray', 'dark blue'),
    ('bg', 'white', 'black'),
    ('focus', 'light red', 'dark gray', 'standout'),
    ('key', 'yellow', 'black'),
    ('help_bold', 'dark blue', 'light gray'),
    ('inserts', 'white', 'dark blue'),
    ('updates', 'white', 'dark green'),
    ('deletes', 'white', 'dark red'),
    ]

# Currently here happens the definition of the views, remember to set queries accordingly
TOP_COLS = {
        'seq_idx': {
            'relid': {
                'n'         : 1,
                'name'      : 'relid',
                'template'  : '%5s ',
                'width'     : 7,
                'mandatory' : True,
                'def_view'  : False,
                'type'      : 'static',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'seq_scan': {
                'n'         : 2,
                'name'      : 'seq_scan',
                'template'  : '%17s ',
                'width'     : 17,
                'mandatory' : False,
                'def_view'  : False,
                'type'      : 'counter',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'seq_tup_read': {
                'n'         : 3,
                'name'      : 'seq_tup_read',
                'template'  : '%17s ',
                'width'     : 17,
                'mandatory' : False,
                'def_view'  : True,
                'type'      : 'counter',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'idx_scan': {
                'n'         : 4,
                'name'      : 'idx_scan',
                'template'  : '%17s ',
                'width'     : 17,
                'mandatory' : True,
                'def_view'  : False,
                'type'      : 'counter',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'idx_tup_fetch': {
                'n'         : 5,
                'name'      : 'idx_tup_fetch',
                'template'  : '%17s ',
                'width'     : 17,
                'mandatory' : True,
                'def_view'  : False,
                'type'      : 'counter',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'relname': {
                'n'         : 6,
                'name'      : 'relname',
                'template'  : '%32s ',
                'width'     : 'pack',
                'mandatory' : False,
                'def_view'  : False,
                'type'      : 'static',
                'align'     : 'left',
                'visible'   : True,
                'wrap'      : 'clip',
            },
        },
        'ins_upd_del': {
            'relid': {
                'n'         : 1,
                'name'      : 'relid',
                'template'  : '%5s ',
                'width'     : 7,
                'mandatory' : True,
                'def_view'  : False,
                'type'      : 'static',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'n_tup_ins': {
                'n'         : 2,
                'name'      : 'n_tup_ins',
                'template'  : '%17s ',
                'width'     : 17,
                'mandatory' : False,
                'def_view'  : True,
                'type'      : 'counter',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'n_tup_upd': {
                'n'         : 3,
                'name'      : 'n_tup_upd',
                'template'  : '%17s ',
                'width'     : 17,
                'mandatory' : False,
                'def_view'  : False,
                'type'      : 'counter',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'n_tup_del': {
                'n'         : 4,
                'name'      : 'n_tup_del',
                'template'  : '%17s ',
                'width'     : 17,
                'mandatory' : True,
                'def_view'  : False,
                'type'      : 'counter',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'relname': {
                'n'         : 5,
                'name'      : 'relname',
                'template'  : '%32s ',
                'width'     : 'pack',
                'mandatory' : False,
                'def_view'  : False,
                'type'      : 'static',
                'align'     : 'left',
                'visible'   : True,
                'wrap'      : 'clip',
            },
        },
        'table_idx': {
            'indexrelid': {
                'n'         : 1,
                'name'      : 'indexrelid',
                'template'  : '%5s ',
                'width'     : 11,
                'mandatory' : True,
                'def_view'  : False,
                'type'      : 'static',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'idx_scan': {
                'n'         : 2,
                'name'      : 'idx_scan',
                'template'  : '%17s ',
                'width'     : 17,
                'mandatory' : False,
                'def_view'  : True,
                'type'      : 'counter',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'idx_tup_read': {
                'n'         : 3,
                'name'      : 'idx_tup_read',
                'template'  : '%17s ',
                'width'     : 17,
                'mandatory' : False,
                'def_view'  : False,
                'type'      : 'counter',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'idx_tup_fetch': {
                'n'         : 4,
                'name'      : 'idx_tup_fetch',
                'template'  : '%17s ',
                'width'     : 17,
                'mandatory' : True,
                'def_view'  : False,
                'type'      : 'counter',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'relname': {
                'n'         : 5,
                'name'      : 'relname',
                'template'  : '%32s ',
                'width'     : 30,
                'mandatory' : False,
                'def_view'  : False,
                'type'      : 'static',
                'align'     : 'left',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'indexrelname': {
                'n'         : 6,
                'name'      : 'indexrelname',
                'template'  : '%32s ',
                'width'     : 'pack',
                'mandatory' : False,
                'def_view'  : False,
                'type'      : 'static',
                'align'     : 'left',
                'visible'   : True,
                'wrap'      : 'clip',
            },
        },
        'active_queries': {
            'pid': {
                'n'         : 1,
                'name'      : 'pid',
                'template'  : '%32s ',
                'width'     : 7,
                'mandatory' : False,
                'def_view'  : False,
                'type'      : 'static',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'datname': {
                'n'         : 2,
                'name'      : 'datname',
                'template'  : '%32s ',
                'width'     : 10,
                'mandatory' : False,
                'def_view'  : False,
                'type'      : 'static',
                'align'     : 'left',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'usename': {
                'n'         : 3,
                'name'      : 'usename',
                'template'  : '%32s ',
                'width'     : 10,
                'mandatory' : False,
                'def_view'  : False,
                'type'      : 'static',
                'align'     : 'left',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'client_addr': {
                'n'         : 4,
                'name'      : 'client_addr',
                'template'  : '%32s ',
                'width'     : 12,
                'mandatory' : False,
                'def_view'  : False,
                'type'      : 'static',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'query_start': {
                'n'         : 5,
                'name'      : 'query_start',
                'template'  : '%32s ',
                'width'     : 18,
                'mandatory' : False,
                'def_view'  : True,
                'type'      : 'static',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'waiting': {
                'n'         : 6,
                'name'      : 'waiting',
                'template'  : '%32s ',
                'width'     : 8,
                'mandatory' : False,
                'def_view'  : False,
                'type'      : 'static',
                'align'     : 'right',
                'visible'   : False,
                'wrap'      : 'clip',
            },
            'pg_backend_pid': {
                'n'         : 7,
                'name'      : 'pg_backend_pid',
                'template'  : '%32s ',
                'width'     : 15,
                'mandatory' : False,
                'def_view'  : False,
                'type'      : 'static',
                'align'     : 'right',
                'visible'   : False,
                'wrap'      : 'clip',
            },
            'state': {
                'n'         : 8,
                'name'      : 'state',
                'template'  : '%32s ',
                'width'     : 15,
                'mandatory' : False,
                'def_view'  : False,
                'type'      : 'static',
                'align'     : 'left',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'query': {
                'n'         : 9,
                'name'      : 'query',
                'template'  : '%32s ',
                'width'     : None,
                'mandatory' : False,
                'def_view'  : False,
                'type'      : 'static',
                'align'     : 'left',
                'visible'   : True,
                'wrap'      : 'clip',
            },
        },
        'table_io': {
            'relid': {
                'n'         : 1,
                'name'      : 'relid',
                'template'  : '%5s ',
                'width'     : 7,
                'mandatory' : True,
                'def_view'  : True,
                'type'      : 'static',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'hit_pct': {
                'n'         : 2,
                'name'      : 'hit_pct',
                'template'  : '%17s ',
                'width'     : 25,
                'mandatory' : False,
                'def_view'  : True,
                'type'      : 'static',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'heap_blks_hit': {
                'n'         : 3,
                'name'      : 'heap_blks_hit',
                'template'  : '%17s ',
                'width'     : 17,
                'mandatory' : False,
                'def_view'  : False,
                'type'      : 'counter',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'heap_blks_read': {
                'n'         : 4,
                'name'      : 'heap_blks_read',
                'template'  : '%17s ',
                'width'     : 17,
                'mandatory' : True,
                'def_view'  : False,
                'type'      : 'counter',
                'align'     : 'right',
                'visible'   : True,
                'wrap'      : 'clip',
            },
            'relname': {
                'n'         : 5,
                'name'      : 'relname',
                'template'  : '%32s ',
                'width'     : 'pack',
                'mandatory' : False,
                'def_view'  : False,
                'type'      : 'static',
                'align'     : 'left',
                'visible'   : True,
                'wrap'      : 'clip',
            },
        }
    }

# We need our own SectableText, otherwise UP/DOWN button jump to the last
# line of the screen
class SelectableText(urwid.Text):
    def __init__(self, txt='', align='left', wrap='space', layout=None):
        super(SelectableText, self).__init__(txt, align, wrap, layout) 

    def selectable(self):
        return True
    
    def keypress(self, size, key):
        if key == 'up':
            if CANVAS.current_focus > 0:
                CANVAS.current_focus = CANVAS.listbox_new.get_focus()[1]-1
                #For reaction time we set the focus not only in _build_canvas
                CANVAS.listbox_new.set_focus(CANVAS.current_focus)
        elif key == 'down':
            if CANVAS.get_current_focus() < CANVAS.body_row_counter-1:
                CANVAS.current_focus = CANVAS.listbox_new.get_focus()[1]+1
                #For reaction time we set the focus not only in _build_canvas
                CANVAS.listbox_new.set_focus(CANVAS.current_focus)
        else:
            return key


class UI(object):
    def __init__(self, DATA, BUFFER):

        self.width = 0
        self.heigth = 0

        # Get initial data
        self.DATA = DATA

        # The different views/modes we have
        self.mode = 'seq_idx'

        # The Default sortkey
        self.sortkey = self._set_default_sortkey()

        #the data we recieve
        self.data = None

        # Initialize the Buffer with current mode
        self.BUFFER = BUFFER

        # A little break
        self.pause = False
        
        # Get first data
        self.refresh()

        #To specifiy where the "build request" comes from, header, body or footer
        self.origin = None

        #How often should we refresh
        self.refresh_intervall = 5

        # All the urwid objects are saved in here
        self.current_ui = None

        # Saves the current focus in listbox
        self.current_focus = 0

        # Overlay already active?
        self.overlay_active = False

        # Help already active?
        self.help_active = False


    def refresh(self):
        if DEBUG: print 'DEBUG: UI().refresh_window()'
        if not self.pause:
            self.update_all_data()
        self.BUFFER.set_mode(self.mode)
        self.BUFFER.set_sortkey(self.sortkey)
            
            #self.data = self.DATA.pg_get_data(self.mode)
            #self.BUFFER.add_data(int(time.time()), self.data)

        first_t, first_d = self.BUFFER.get_first()
        last_t, last_d = self.BUFFER.get_last()
            #print first_t, first_d
            #print last_t, last_d
            #time.sleep(1)
        delta = self.BUFFER.get_delta(first_t, first_d, last_t, last_d)
        self.data = self.BUFFER.get_sorted(delta)

    def update_all_data(self):
        for view in TOP_COLS.keys():
            self.BUFFER.set_mode(view)
            
            self.data = self.DATA.pg_get_data(view)
            self.BUFFER.add_data(int(time.time()), self.data)



    def keystroke(self, input):
        if input in ('q', 'Q'):
            if self.overlay_active or self.help_active:
                self.overlay_active = False
                self.help_active = False
            else:
                raise urwid.ExitMainLoop()

        if input in ('s', 'S'):
            self.switch_sortkey()

        if input in ('v', 'V'):
            self.switch_view()

        if input in ('u', 'U'):
            ''' Move ListBox up, don't go further then index 0 '''
            if self.current_focus > 0:
                self.current_focus = self.listbox_new.get_focus()[1]-1
                #For reaction time we set the focus not only in _build_canvas
                self.listbox_new.set_focus(self.current_focus)

        if input in ('d', 'D'):
            ''' Move ListBox down, don't go further then listbox length '''
            #print self.listbox_new.get_focus()[1]
            if self.current_focus < self.body_row_counter-1:
                self.current_focus = self.listbox_new.get_focus()[1]+1
                #For reaction time we set the focus not only in _build_canvas
                self.listbox_new.set_focus(self.current_focus)

        if input in ('c', 'C'):
            ''' Clear history_buffer '''
            self.BUFFER.reset()
            
        if input in ('enter',):
            ''' Enable Overlay (details) '''
            self.overlay_active = True
            #self.detail_view()

        if input in ('h',):
            ''' Show help for current view '''
            self.help_active = True

        if input in ('p', 'P'):
            ''' Pause refreshing '''
            if not self.pause:
                self.pause = True
            else:
                self.pause = False

    def switch_sortkey(self):
        ''' If you want to switch column of ordering '''
        list_of_ordering = []
        for i in range(0, len(TOP_COLS[self.mode])+1):
            for row in TOP_COLS[self.mode]:
                if TOP_COLS[self.mode][row]['n'] == i:
                    list_of_ordering.append(TOP_COLS[self.mode][row]['name'])
                    break
        current_index = list_of_ordering.index(self.sortkey)
        if current_index+1 < len(list_of_ordering):
            self.sortkey = list_of_ordering[current_index+1]
        else:
            self.sortkey = list_of_ordering[0]

    def _set_default_sortkey(self):
        ''' On switch_view we need to set default sortkey '''
        for i in TOP_COLS[self.mode]:
            if TOP_COLS[self.mode][i]['def_view']:
                default_sortkey = TOP_COLS[self.mode][i]['name']

        return default_sortkey

    def switch_view(self):
        '''Switch the view between our predefined collections'''
        if DEBUG: print 'DEBUG: UI().switch_view()'

        list_of_views = TOP_COLS.keys()
        current_index = list_of_views.index(self.mode)

        if current_index+1 < len(list_of_views):
            self.mode = list_of_views[current_index+1]
        else:
            self.mode = list_of_views[0]
        
        self.current_focus = 0
        self.BUFFER.set_mode(self.mode)
        #self.BUFFER.reset()
        self.sortkey = self._set_default_sortkey()
    
    def get_current_focus(self):
        return self.current_focus

    def set_cols_rows(self, cols_rows=()):
        ''' set screen width, heigth '''
        self.width, self.GGGheigth = cols_rows

    def get_heigth(self):
        ''' return screen heigth '''
        return self.heigth

    def get_width(self):
        ''' return screen width '''
        return self.width

    def header(self):
        ''' Build the header '''
        header_list = []
        
        first_row = urwid.Columns([
            urwid.AttrMap(
                urwid.Text("Postgres Version: %s" % BUFFER.database_version[0], 'left', 'clip'), self.origin),
            urwid.AttrMap(
                urwid.Text("Inserts: INTEGER", 'right', 'clip'), 'inserts'),
        ])
        second_row = urwid.Columns([
            urwid.AttrMap(
                urwid.Text("Database: %s" % args.database, 'left', 'clip'), self.origin),
            urwid.AttrMap(
                urwid.Text("Updates: INTEGER", 'right', 'clip'), 'updates'),
        ])
        third_row = urwid.Columns([
            urwid.AttrMap(
                urwid.Text("View: %s" % self.mode, 'left', 'clip'), self.origin),
            urwid.AttrMap(
                urwid.Text("Deletes: INTEGER", 'right', 'clip'), 'deletes'),
        ])

        overall_row = urwid.Columns([
            urwid.AttrMap(
                urwid.Text("Inserts: INTEGER", 'center', 'clip'), 'inserts'),
            urwid.AttrMap(
                urwid.Text("Updates: INTEGER", 'center', 'clip'), 'updates'),
            urwid.AttrMap(
                urwid.Text("Deletes: INTEGER", 'center', 'clip'), 'deletes'),
        ])
        
        self.origin = 'header'
        header_row = self._generate_row()

        #print header_row
        #time.sleep(2)

        header_list.append(first_row)
        header_list.append(second_row)
        header_list.append(third_row)
        #header_list.append(overall_row)
        header_list.append(header_row)
        return urwid.Pile(header_list)

    def body(self):
        ''' Build the body '''
        if DEBUG: print 'DEBUG: UI().body()'
        self.origin = 'body'
        #Give data to _generate_row and put everything in a list
        body_rows = []
        counter = 0
        for i in self.data:
            body_rows.append(self._generate_row(i[1]))
            counter += 1

        self.body_row_counter = counter
        return body_rows

    def footer(self):
        ''' Build the footer '''
        self.origin = 'footer'

        row = urwid.Columns([
                urwid.AttrMap(
                    urwid.Text(('key', "(S)ortkey"), 'left', 'clip'), self.origin),
                urwid.AttrMap(
                    urwid.Text("(V)iew: " + self.mode, 'left', 'clip'), self.origin),
                urwid.AttrMap(
                    urwid.Text("(U)p", 'left', 'clip'), self.origin),
                urwid.AttrMap(
                    urwid.Text("(D)own", 'left', 'clip'), self.origin),
                urwid.AttrMap(
                    urwid.Text("(Q)uit", 'left', 'clip'), self.origin),
                ]
                )
        blank = urwid.Divider()
        footer_sortkey = [('key', 'S'), (self.origin, 'ortkey ')]
        footer_view = [('key', 'V'), (self.origin, 'iew ' + self.mode + ' ')]
        footer_up = [('key', 'U'), (self.origin, 'p ')]
        footer_down = [('key', 'D'), (self.origin, 'own ')]
        footer_quit = [('key', 'Q'), (self.origin, 'uit ')]
        footer_detail = [('key', 'Enter'), (self.origin, 'Details ')]
        footer_help = [('key', 'H'), (self.origin, 'elp ')]
        footer_clear = [('key', 'C'), (self.origin, 'lear ')]
        footer_pause = [('key', 'P'), (self.origin, 'ause ')]
        row = urwid.AttrMap(
                    urwid.Columns([
                        ('pack', urwid.Text(footer_help)),
                        ('pack', urwid.Text(footer_sortkey)),
                        ('pack', urwid.Text(footer_view)),
                        ('pack', urwid.Text(footer_up)),
                        ('pack', urwid.Text(footer_down)),
                        ('pack', urwid.Text(footer_detail)),
                        ('pack', urwid.Text(footer_clear)),
                        ('pack', urwid.Text(footer_pause)),
                        ('pack', urwid.Text(footer_quit)),
                ]), self.origin)
        return row

    def _generate_row(self, raw_data=None):
        ''' Generate each column out of the template constant '''
        if DEBUG: print 'DEBUG: UI()._generate_row()'
        n_items = len(TOP_COLS[self.mode])
        new_row = []

        for i in range(0, n_items):
            current_col = None
            visible = False
            
            # We need to get the current_col from the elements in the constant
            for row in TOP_COLS[self.mode]:
                if TOP_COLS[self.mode][row]['n'] == i+1:
                    current_col = TOP_COLS[self.mode][row]['name']

                    # Header or data?
                    if raw_data: # Data
                        current_string = raw_data[current_col]
                    else: # Header
                        current_string = current_col

                    break #Break the loop if we have what we want
                
            # Is this column visible?
            if TOP_COLS[self.mode][current_col]['visible']:
                visible = True
            else:
                visible = False

            # Get width
            current_width = TOP_COLS[self.mode][current_col]['width']
            current_align = TOP_COLS[self.mode][current_col]['align']
            current_wrap = TOP_COLS[self.mode][current_col]['wrap']

            if visible:
                if raw_data:
                    #Fill the columns
                    if not current_width:
                        new_row.append(SelectableText(str(current_string)+" ", current_align, current_wrap))
                    else:
                        new_row.append((current_width, SelectableText(str(current_string)+" ", current_align, current_wrap)))
                else:
                    if current_string == self.sortkey:
                        new_row.append((current_width, urwid.AttrMap(urwid.Text(str(current_string)+" ", current_align, current_wrap), 'header_focus')))
                    else:
                        if not current_width:
                            new_row.append(urwid.Text(str(current_string)+" ", current_align, current_wrap))
                        else:
                            new_row.append((current_width, urwid.Text(str(current_string)+" ", current_align, current_wrap)))

        #Putting AttrMap around Columns so that it acts as one complete row (highlighting)
        return urwid.AttrMap(urwid.Columns(new_row), self.origin, 'focus')

    def _build_canvas(self):
        '''
        This is where the magic happens
        Everything is put together
        Looks like this:

        Frame(
            AttrMap(body),
            header,
            footer,
                ListBox(
                    SimpleListWalker(
                    [
                        AttrMap(Columns([Text, Text, Text])),

                        AttrMap(Columns([Text, Text, Text]))
                    ]
                    )
                )
            )
        '''
        #All the content
        self.listbox_new = urwid.ListBox(
                urwid.SimpleListWalker(
                    self.body())
                )

        frame = urwid.Frame(
                urwid.AttrMap(
                    self.listbox_new, 'body'),
                    header=self.header(),
                    footer=self.footer())

        main = frame

        #Show the detail view of the current view, e.g. details of row
        if self.overlay_active:
            overlay = urwid.Overlay(
                self.simple_overlay("I am the awesome title", str(self.detail_view())), frame,
                align='center', width=('relative', 80),
                valign='middle', height=('relative', 60),
                min_width=20, min_height=9)
            main = overlay

        if self.help_active:
            help_overlay = urwid.Overlay(
                    self.simple_overlay("Help", self.get_help_view()), frame,
                    align='center', width=('relative', 80),
                    valign='middle', height=('relative', 60),
                    min_width=20, min_height=9
            )
            main = help_overlay



        self.listbox_new.set_focus(self.current_focus)

        return main
        #return frame


    def simple_overlay(self, title, content):
        '''A simple overlay function for displaying text'''
        txt = urwid.Text(content)
        fill = urwid.Filler(txt, 'top')

        olay = urwid.AttrMap(
                urwid.LineBox(
                    fill, title), 'body')
        return olay

    def get_help_view(self):
        if self.mode == 'seq_idx':
            help_text = [
                "The ", ('help_bold', "seq_idx"), " view shows you, "
                "how you currently retrieve your data.\n",
                "If you see an incredible high amount of ",
                ('help_bold', "seq_tup_read"),
                " this is a first hint for a missing index. Usually you want "
                "to avoid a lot of loading through sequential scans.\n",
                "But be aware that putting an index on all table columns will "
                "also lead to an decreased performance, as for every adding of"
                " an entry, all indexes have to be touched too.\n",
                "\n\n",
                "Explanation of Fields:\n",
                ('help_bold', "relid"),
                ": The relation id of the table shown\n",
                ('help_bold', "seq_scan"),
                ": Shows you the initiated sequential scans\n",
                ('help_bold', "seq_tup_read"),
                ": Shows you the retrieved rows through seq_scan\n",
                ('help_bold', "idx_scan"),
                ": Shows you the initiated index scans\n",
                ('help_bold', "idx_tup_fetch"),
                ": Shows you the tuples recieved through index scan\n",
                ('help_bold', "relname"),
                ": The name of the table\n",
                ]
        elif self.mode == 'ins_upd_del':
            help_text = [
                    "The ", ('help_bold', "ins_upd_del"), " view shows you, "
                    "what kind of actions you are currently doing in your DB"
                    "\n\n"
                    "Explanation of Fields:\n",
                    ('help_bold', "relid"),
                    ": The relation id of the table shown\n",
                    ('help_bold', "n_tup_ins"),
                    ": Counter of inserts\n",
                    ('help_bold', "n_tup_upd"),
                    ": Counter of updates\n",
                    ('help_bold', "n_tup_del"),
                    ": Counter of deletes\n",
                    ('help_bold', "relname"),
                    ": The name of the table\n",
                    ]
        elif self.mode == 'table_idx':
            help_text = [
                    "The ", ('help_bold', "table_idx"), " view shows you, "
                    "a detailed view of each index."
                    "\n\n"
                    "Explanation of Fields:\n",
                    ('help_bold', "relid"),
                    ": The relation id of the table shown\n",
                    ('help_bold', "idx_scan"),
                    ": Shows you the initiated index scans\n",
                    ('help_bold', "idx_tup_read"),
                    ": Shows you the tupley recieved from live rows\n",
                    ('help_bold', "idx_tup_fetch"),
                    ": Shows you the tuples recieved through index scan\n",
                    ('help_bold', "relname"),
                    ": The name of the table\n",
                    "This explanation is still under construction"
                    ]
        else:
            help_text = [
                    "No help available"
                    ]
        return help_text


    def detail_view(self):
        #current_row = BUFFER.get_specific()
        current_row = self.data[self.current_focus][0]
        self.DATA.special_key = current_row
        return current_row

    def get_ui(self):
        self.current_ui = self._build_canvas()
        return self.current_ui


class Data(object):
    def __init__(self):
        if DEBUG: print 'DEBUG: Data().__init__()'
        self.pg_conn = self.pg_connect()

        #TODO: Ugly name, for key of special queries, like tab_idx_detail
        self.special_key = None

    def pg_connect(self,
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database):
        '''
        Small connection class
        If no password is suplied we try the default postgres user
        and expect a setted pg_ident or something similar
        '''
        try:
            if password:
                conn = psycopg2.connect(
                    database=database,
                    user=user,
                    port=port,
                    password=str(password),
                    host=host
                    #connection_factory = psycopg2.extras.DictConnection
                    )
            else:
                conn = psycopg2.connect(
                    database=database,
                    user=user,
                    port=port,
                    password="",
                    host=None
                    #connection_factory = psycopg2.extras.DictConnection
                    )

        except psycopg2.Error, psy_err:
            print "The connection is not possible!"
            print psy_err
            print psycopg2.Error
            if host is None:
                raise psy_err

        conn.set_isolation_level(0)
        return conn

    def pg_get_data(self, mode):
        '''
        The "upper" method which is called from "outside",
        here the decision is done which queries will be executed
        '''

        if mode == 'seq_idx':
            return self._pg_get_seq_idx()
        elif mode == 'ins_upd_del':
            return self._pg_get_ins_upd_del()
        elif mode == 'table_idx':
            return self._pg_get_table_idx()
        elif mode == 'active_queries':
            return self._pg_get_active_queries()
        elif mode == 'table_io':
            return self._pg_get_table_io()
        else:
            print "NO MODE SET, EXITING"
            time.sleep(2)
            sys.exit(1)

    def _pg_get_version(self):
        ''' Get current postgresql server version '''

        query = """
        SHOW server_version        
        """
        cur = self.pg_conn.cursor()
        cur.execute(query)
        ret = cur.fetchall()
        return ret

    def _pg_get_seq_idx(self):
        '''
        Return information of:
        relid: Table ID
        relname: Table Name
        seq_scan: Initiated sequential scans
        seq_tup_read: Recieved tuples by seq_scan
        idx_scan: Initiated index scans
        idx_tup_fetch: Recieved tuples by idx_scan (TODO check idx_tup_read)
        '''
        query = """
        SELECT
            relid,
            seq_scan,
            seq_tup_read,
            idx_scan,
            idx_tup_fetch,
            relname
        FROM
            pg_stat_all_tables
            """
        cur = self.pg_conn.cursor()
        cur.execute(query)
        ret = cur.fetchall()
        return ret

    def _pg_get_ins_upd_del(self):
        '''
        Return information of:
        relid: Table ID
        relname: Table Name
        n_tup_ins: Inserts
        n_tup_upd: Updates
        n_tup_del: Deletes
        '''
        query = """
            SELECT
                relid,
                n_tup_ins,
                n_tup_upd,
                n_tup_del,
                relname
            FROM
                pg_stat_all_tables
            """
        cur = self.pg_conn.cursor()
        cur.execute(query)
        ret = cur.fetchall()
        return ret

    def _pg_get_table_idx(self):
        query = """
            SELECT
                indexrelid,
                idx_scan,
                idx_tup_read,
                idx_tup_fetch,
                relname,
                indexrelname
            FROM
                pg_stat_all_indexes
            """
        cur = self.pg_conn.cursor()
        cur.execute(query)
        ret = cur.fetchall()
        return ret

    #Just for testing
    def _pg_get_table_idx_detail(self):
        query = """
            SELECT
                indexrelid,
                idx_scan,
                idx_tup_read,
                idx_tup_fetch,
                indexrelname
            FROM
                pg_stat_all_indexes
            WHERE
                relid = {0}
            """.format(self.secial_key)
        cur = self.pg_conn.cursor()
        cur.execute(query)
        ret = cur.fetchall()
        return ret

    def _pg_get_active_queries(self):
        query = """
            SELECT 
                pid, 
                datname, 
                usename, 
                client_addr,
                clock_timestamp() - query_start, 
                waiting,
                pg_backend_pid(), 
                state,
                query 
            FROM 
                pg_stat_activity
            WHERE 
                state NOT LIKE 'idle';
        """
        cur = self.pg_conn.cursor()
        cur.execute(query)
        ret = cur.fetchall()
        return ret

    def _pg_get_table_io(self):
        query = """
            SELECT 
                relid,
                cast(heap_blks_hit as numeric) / (heap_blks_hit + heap_blks_read) AS hit_pct,
                heap_blks_hit,
                heap_blks_read,
                relname
            FROM 
                pg_statio_all_tables 
            WHERE 
                (heap_blks_hit + heap_blks_read)>0 
            ORDER BY 
                hit_pct; 
        """
        cur = self.pg_conn.cursor()
        cur.execute(query)
        ret = cur.fetchall()
        return ret

    def _set_buffer(self, query):
        pass

class Buffer(object):
    def __init__(self, mode=None):
        if DEBUG: print'DEBUG: Buffer().__init__()'
        self.begin = int(time.time())

        # Because of different buffer types, like seq. reads,
        # and querys, we need a switch
        self.mode = mode

        # Sortkey, which column should be sorted
        self.sortkey = None

        # The overall history buffer, all items are stored here
        self.history_buffer = {}

        # The sum history buffer, all db wide objects are summarized in here
        self.sum_history_buffer = {}

        # The previous timestamp, needed to delete the entry from buffer
        self.previous_timestamp = None

        # Get database version once, i suggest it doesn't change during yapgt execution :D
        self.database_version = DATA._pg_get_version()

    def add_data(self, timestamp=None, rows=[]):
        ''' Save data in history_buffer to access it later '''
        if DEBUG: print 'DEBUG: Buffer().add_data()'

        if self.mode in ('seq_idx', 'ins_upd_del', 'table_idx', 'active_queries', 'table_io'):
            row_buffer = {} # actually all rows get saved here
            sum_row_buffer = {}
            col_buffer = {} # we need a temporary dict to save current rowelements
            sum_col_buffer = {}
            time_buffer = {}
            sum_time_buffer = {}

            for row in rows:
                col_buffer.clear() #clear the buffer
                for col in TOP_COLS[self.mode]:
                    # create a dictionary based on our TOP_COLS
                    # this way we don't need to add new buffer rules
                    col_buffer[TOP_COLS[self.mode][col]['name']] = row[TOP_COLS[self.mode][col]['n']-1] or 0
        
                    if TOP_COLS[self.mode][col]['type'] is 'counter':
                        #print row[TOP_COLS[self.mode][col]['n']-1]
                        sum_col_buffer[TOP_COLS[self.mode][col]['name']] = sum_col_buffer.get(TOP_COLS[self.mode][col]['name'], 0) + (row[TOP_COLS[self.mode][col]['n']-1] or 0)
                        #print sum_col_buffer[TOP_COLS[self.mode][col]['name']]
                        #time.sleep(1)

                # use copy to copy a dict, really do it...
                row_buffer[row[0]] = col_buffer.copy()
            
            time_buffer[timestamp] = row_buffer.copy()
            if self.mode not in self.history_buffer.keys():
                self.history_buffer[self.mode] = time_buffer.copy()
            else:
                self.history_buffer[self.mode].update(time_buffer.copy())
            
            sum_time_buffer[timestamp] = sum_col_buffer.copy()
            if self.mode not in self.sum_history_buffer.keys():
                self.sum_history_buffer[self.mode] = time_buffer.copy()
            else:
                self.sum_history_buffer[self.mode].update(time_buffer.copy())
            
            #print self.history_buffer[self.mode][timestamp]
            # copy current row buffer into the history_buffer
            #self.history_buffer[timestamp] = row_buffer

        # Clean the mess up, if more than 2 keys are in the dict,
        # this means we have first and last and one between.
        # Delete the one between, otherwise we hit memorylimit
        # after several hours
        if len(self.history_buffer[self.mode]) > 2:
            first = min(self.history_buffer[self.mode].keys())
            last = max(self.history_buffer[self.mode].keys())
            #print ">2"
            for i in self.history_buffer[self.mode].keys():
                if i not in (first, last) and i < last - 10:
                    ##print "-10"
                    del self.history_buffer[self.mode][i]
                #time.sleep(1)
        #Save the timestamp for next run
        #self.previous_timestamp = timestamp
        #print self.get_average(60, timestamp)


    def get_average(self, seconds, timestamp):
        all_values = 0
        counter = 0
        for i in self.sum_history_buffer[self.mode]:
            print i
            time.sleep(1)
            if i-seconds < timestamp:
                print "foobar" 
                time.sleep(1)

    def get_first(self):
        ''' Return the first dataset '''
        if DEBUG: print 'DEBUG: Buffer().get_first()'

        first_timestamp = min(self.history_buffer[self.mode].keys())

        return first_timestamp, self.history_buffer[self.mode][first_timestamp]

    def get_last(self):
        ''' Return the last dataset '''
        if DEBUG: print 'DEBUG: Buffer().get_last()'

        last_timestamp = max(self.history_buffer[self.mode].keys())

        return last_timestamp, self.history_buffer[self.mode][last_timestamp]

    def get_specific(self, relid):
        ''' Return a single row '''

        last_t, last_d = self.get_last()

        return last_d

    def get_delta(self, first_timestamp=None, first_dataset=None,
            last_timestamp=None, last_dataset=None):
        ''' Calculate the increase between given datasets '''
        if DEBUG: print 'DEBUG: Buffer().get_delta()'
        new_dict = {}

        if self.mode in ('seq_idx', 'ins_upd_del', 'table_idx', 'active_queries', 'table_io'):
            row_buffer = {} # All rows go in here
            col_buffer = {} # we need a temporary dict to save current rowelements

            for row in last_dataset:
                for col in last_dataset[row]:
                    if TOP_COLS[self.mode][col]['type'] == 'static': # We don't need to substract
                        # create a dictionary based on our TOP_COLS
                        # this way we don't need to add new buffer rules
                        col_buffer[TOP_COLS[self.mode][col]['name']] = last_dataset[row][col]
                    elif TOP_COLS[self.mode][col]['type'] == 'counter': # We have to get the delta
                        # create a dictionary based on our TOP_COLS
                        # this way we don't need to add new buffer rules
                        col_buffer[TOP_COLS[self.mode][col]['name']] = last_dataset[row][col] - first_dataset[row][col]
                # use copy to copy a dict, really do it...
                row_buffer[row] = col_buffer.copy()
                if self.mode in ('active_queries'):
                    row_buffer[row] = self.get_prepared_data(col_buffer.copy())
                else:
                    row_buffer[row] = col_buffer.copy()
            #TODO Actually i don't need the calculation, have to check what i to i sort method
            new_dict[last_timestamp - first_timestamp] = row_buffer

        return new_dict

    def get_sorted(self, tosort_d={}):
        ''' give an ordered output '''
        if DEBUG: print 'DEBUG: Buffer().get_sorted()'
        #TODO: Sort_key
        #TODO: Better way -.-
        for i in tosort_d:
            d = tosort_d[i]

        #damn, how does this work O_o
        return sorted(d.items(), key=lambda x: x[1][self.sortkey], reverse=True)

    def get_prepared_data(self, raw_dict):
        
        if self.mode in ('active_queries'):
            current_query = raw_dict['query']
            current_query = current_query.replace('\t', ' ').replace('\n', ' ').strip()
            raw_dict['query'] = re.sub('\s+', ' ', current_query)
            return raw_dict

    def reset(self):
        self.history_buffer.clear()

    def set_mode(self, mode):
        self.mode = mode

    def get_mode(self):
        return self.mode

    def set_sortkey(self, sortkey):
        self.sortkey = sortkey

    def get_sortkey(self):
        return self.sortkey

def refresh(_loop, _data):
    if DEBUG: print 'DEBUG: refresh_window() callback'

    CANVAS.set_cols_rows(_loop.screen.get_cols_rows())
    CANVAS.refresh()

    _loop.widget = CANVAS.get_ui()

    #_loop.draw_screen()
    _loop.set_alarm_in(1, refresh)

def exit_on_q(input):
    if input in ('q', 'Q'):
        raise urwid.ExitMainLoop()

def keep(a):
    ''' A little debug function '''
    with open("/tmp/main.log", "a") as f:
        f.write(str(a)+"\n")

DATA = Data()
BUFFER = Buffer()
CANVAS = UI(DATA, BUFFER)
def main():

    loop = urwid.MainLoop(widget=CANVAS.get_ui(),
            palette=palette,
            unhandled_input=CANVAS.keystroke,
            handle_mouse=False)

    loop.set_alarm_in(1, refresh)

    loop.run()


if __name__ == "__main__":
    main()
