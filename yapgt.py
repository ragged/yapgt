#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import urwid
import psycopg2
import argparse
import utils.connect


#Some Options
parser = argparse.ArgumentParser(
        description="YAPGT - Yet Another PostgreSQL Tool")
parser.add_argument("-H", "--host", default="localhost", 
        dest="host", help="Host to connect to (not tested yet)")
parser.add_argument("-p", "--port", default="5432", 
        dest="port", help="Which port to connect to")
parser.add_argument("-d", "--database", dest="database",
        help="From which database should the statistics be collected")
parser.add_argument("-u", "--user", default="postgres", 
        dest="user", help="Which user shall connect")
parser.add_argument("-w", "--password", dest="password", 
        help="The password of the DB")
args = parser.parse_args()

UPDATE_INTERVAL = 1
DEBUG = True



class SelectableText(urwid.Text):
    def __init__(self, txt='', align='left', wrap='space', layout=None):
        super(SelectableText, self).__init__(txt, align, wrap, layout) 

    def selectable(self):
        return True
    
    def keypress(self, size, key):
        return key

class Model(object):
    """
    Calculations happen in here, also the gathering of the data.
    The data should be presented in a final state. Like an API. 
    """
    def __init__(self):
        if DEBUG: keep("Model().__init__()")
        self.begin = int(time.time())
        self.query = "" # current query
        self.meta = [] # current meta data
        self.view = "" # current view name

        #self.modes = ['seq_idx', 'ins_upd_del', 'table_idx', 'table_io']
        self.modes = ['seq_idx', 'ins_upd_del']
        # Connection object
        #self.pg_conn = self.pg_connect()
        self.pg_conn = utils.connect.pg_connect(
                host=args.host,
                port=args.port,
                user=args.user,
                password=args.password,
                database=args.database)

        #The main buffer for all the fancy statistics we safe
        self.history_buffer = {}
        self.update_all() # We want to update all data right in the beginning

    def update_all(self):
        """ Just get all data we have in all modes.
            This way we can update all views at once, 
            e.g. in overlay modes..."""
        if DEBUG: keep("Model().get_all()")
        for i in self.modes:
            self.set_mode(i)
            self.get_data()

    def set_mode(self, m):
        """ Set mode """
        if DEBUG: keep("Model().set_mode()")
        self.current_mode = m
        self._set_meta()

    def get_modes(self):
        """ Return all modes that are defined in the Model()"""
        if DEBUG: keep("Model().get_modes()")
        return self.modes
    
    def get_mode(self):
        """ Return current active mode """
        if DEBUG: keep("Model().get_mode()")
        return self.current_mode
    
    def _set_meta(self):
        if DEBUG: keep("Model().switch_meta()")
        if self.current_mode == 'seq_idx':
            self._get_seq_idx()
        elif self.current_mode == 'ins_upd_del':
            self._get_ins_upd_del()

    def get_meta(self):
        if DEBUG: keep("Model().get_meta()")
        return self.meta 
    
    def get_data(self):
        if DEBUG: keep("Model().get_data()")
        #self.buffer_data()
        keep(self.buffer_data())
        return self._get_delta()
        #return str(self.buffer_data())
        #return str(time.time())

    def buffer_data(self):
        ''' The data has to be saved '''
        if DEBUG: keep("Model().buffer_data()")

        column_headers, data = utils.connect.pg_get_data(self.pg_conn, self.query)
        #The current timestamp, we need it to differentiate between oldest and newest data
        timestamp = int(time.time() )

        row_buffer = {} # actually all rows get saved here
        sum_row_buffer = {}
        col_buffer = {} # we need a temporary dict to save current rowelements
        sum_col_buffer = {}
        time_buffer = {}
        sum_time_buffer = {}

        for row in data:
            col_buffer.clear() #clear the buffer
            for col in column_headers:
                # create a dictionary based on our column_headers
                # this way we don't need to add new buffer rules
                col_buffer[col] = row[column_headers.index(col)] or 0
        
            # use copy to copy a dict, really do it...
            row_buffer[row[0]] = col_buffer.copy()
        
        time_buffer[timestamp] = row_buffer.copy()
        if self.current_mode not in self.history_buffer.keys():
            self.history_buffer[self.current_mode] = time_buffer.copy()
        else:
            self.history_buffer[self.current_mode].update(time_buffer.copy())
        
        #sum_time_buffer[timestamp] = sum_col_buffer.copy()
        #if self.current_mode not in self.sum_history_buffer.keys():
        #    self.sum_history_buffer[self.current_mode] = time_buffer.copy()
        #else:
        #    self.sum_history_buffer[self.current_mode].update(time_buffer.copy())
        
        # Clean the mess up, if more than 2 keys are in the dict,
        # this means we have first and last and one between.
        # Delete the one between, otherwise we hit memorylimit
        # after several hours
        if len(self.history_buffer[self.current_mode]) > 2:
            first = min(self.history_buffer[self.current_mode].keys())
            last = max(self.history_buffer[self.current_mode].keys())
            keep("first: "+str(first))
            keep("last: "+str(last))
            for i in self.history_buffer[self.current_mode].keys():
                if i not in (first, last) and i < last:
                    ##print "-10"
                    del self.history_buffer[self.current_mode][i]
            #time.sleep(1)
        #Save the timestamp for next run
        #self.previous_timestamp = timestamp
        #print self.get_average(60, timestamp)
        
        
        for i in self.history_buffer['seq_idx']:
            keep("history_buffer: " + str(i))
        keep(self.history_buffer)
    
    def _get_first(self):
        ''' Return the first dataset '''
        if DEBUG: keep('Model()._get_first()')

        first_timestamp = min(self.history_buffer[self.current_mode].keys())

        return first_timestamp, self.history_buffer[self.current_mode][first_timestamp]

    def _get_last(self):
        ''' Return the last dataset '''
        if DEBUG: keep('Model()._get_last()')

        last_timestamp = max(self.history_buffer[self.current_mode].keys())

        return last_timestamp, self.history_buffer[self.current_mode][last_timestamp]

    def _get_top_cols(self):
        ''' Create list for header '''
        if DEBUG: keep('Model()._get_top_cols()')

        top_cols = {}
        for i in self.meta:
            pass

    def _get_delta(self):
        ''' Calculate the increase between given datasets '''
        if DEBUG: keep("Model()._get_delta()")
        new_dict = {}
        
        first_timestamp, first_dataset = self._get_first()
        last_timestamp, last_dataset = self._get_last()

        row_buffer = {} # All rows go in here
        col_buffer = {} # we need a temporary dict to save current rowelements

        for row in last_dataset:
            for col in last_dataset[row]:
                if self.meta[col]['type'] == 'static': # We don't need to substract
                    # create a dictionary based on our TOP_COLS
                    # this way we don't need to add new buffer rules
                    col_buffer[self.meta[col]['name']] = last_dataset[row][col]
                elif self.meta[col]['type'] == 'counter': # We have to get the delta
                    # create a dictionary based on our TOP_COLS
                    # this way we don't need to add new buffer rules
                    col_buffer[self.meta[col]['name']] = last_dataset[row][col] - first_dataset[row][col]
            # use copy to copy a dict, really do it...
            row_buffer[row] = col_buffer.copy()
            if self.current_mode in ('active_queries'):
                row_buffer[row] = self.get_prepared_data(col_buffer.copy())
            else:
                row_buffer[row] = col_buffer.copy()
        #TODO Actually i don't need the calculation, have to check what i to i sort method
        new_dict[last_timestamp - first_timestamp] = row_buffer

        return new_dict

    def _get_seq_idx(self):
        if DEBUG: keep("Model()._get_seq_idx()")
        
        self.name = "seq_idx"

        self.query = """
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

        self.meta = {
                'relid':{
                    'id'        : 0,
                    'name'      :'relid',
                    'template'  : '%5s ',
                    'width'     : 7,
                    'mandatory' : True,
                    'def_view'  : False,
                    'type'      : 'static',
                    'align'     : 'right',
                    'visible'   : True,
                    'wrap'      : 'clip',
                },
                'seq_scan':{
                    'id'        : 1,
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
                'seq_tup_read':{
                    'id'        : 2,
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
                'idx_scan':{
                    'id'        : 3,
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
                'idx_tup_fetch':{
                    'id'        : 4,
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
                'relname':{
                    'id'        : 5,
                    'name'      : 'relname',
                    'template'  : '%32s ',
                    'width'     : 'pack',
                    'mandatory' : False,
                    'def_view'  : False,
                    'type'      : 'static',
                    'align'     : 'left',
                    'visible'   : True,
                    'wrap'      : 'clip',
                },}

    def _get_ins_upd_del(self):
        if DEBUG: keep("Model()._get_ins_upd_del()")

        self.name = "ins_upd_del"

        self.query = """
            SELECT
                relid,
                n_tup_ins,
                n_tup_upd,
                n_tup_del,
                relname
            FROM
                pg_stat_all_tables
            """
        
        self.meta = {
                'relid':{
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
                'n_tup_ins':{
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
                'n_tup_upd':{
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
                'n_tup_del':{
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
                'relname':{
                    'name'      : 'relname',
                    'template'  : '%32s ',
                    'width'     : 'pack',
                    'mandatory' : False,
                    'def_view'  : False,
                    'type'      : 'static',
                    'align'     : 'left',
                    'visible'   : True,
                    'wrap'      : 'clip',
                },}
        



class View(urwid.WidgetWrap):
    """
    A class for the general view of the application. All widgets
    get handled in here.
    """
    palette = [
        ('body', 'black', 'light gray'),
        ('button_normal', 'yellow', 'black'),
        ('button_select', 'light red', 'black'),
            ]

    def __init__(self, controller):
        if DEBUG: keep("View().__init__({})".format(controller))
        self.controller = controller
        
        self.mode = self.controller.get_mode()
        self.frame = None
        self.redraw_window = False
        
        # As we need a initial starting point
        urwid.WidgetWrap.__init__(self, self.main_window())

    def set_redraw_window(self, state):
        """ If True it redraws everything, beware of losing all your focus """
        if DEBUG: keep("View().set_redraw_window(%s)" % state)
        self.redraw_window = state

    def update(self, window_refresh=False):
        """ Add new data to the views if an update occured """
        if DEBUG: keep("View().update()")
        keep(self.controller.get_data())
        


        self.window_refresh = window_refresh
        self._w = self.main_window()
    
    def on_button_click(self, button):
        if DEBUG: keep("View().on_button_click()")
        pass

    def on_mode_change(self):
        if DEBUG: keep("View().on_mode_change()")
        pass

    def button(self, t, fn):
        if DEBUG: keep("View().button()")
        w = urwid.Button(t, fn)
        w = urwid.AttrMap(w, 'button_normal', 'button_select')
        return w
    
    def _get_order(self, meta):
        if DEBUG: keep("View().get_order()")
        """ This is to produce the right order of columns"""
    
        order_list = []

        while len(order_list) < len(meta): # TODO Move to update()?
            for sub_meta in meta:
                if meta[sub_meta]['id'] == len(order_list):
                    order_list.append(meta[sub_meta]['name'])
                    keep(order_list)
        return order_list

    def row(self):
        if DEBUG: keep("View().row({})")
        """        
                  'relname':{
                    'name'      : 'relname',
                    'template'  : '%32s ',
                    'width'     : 'pack',
                    'mandatory' : False,
                    'def_view'  : False,
                    'type'      : 'static',
                    'align'     : 'left',
                    'visible'   : True,
                    'wrap'      : 'clip',
                },P
        """
        #TODO Move to update() ?
        meta = self.controller.get_meta()
        data = self.controller.get_data()
       
        column_order = self._get_order(meta)

        columns = []
        row = []
        rows = []
          
        # Here it can be a little bit tricky
        # Currently we have the data and meta, to add correct information
        # for each column we need the meta data, those will match for 
        # every row in the current view. 
        # We iterate through the data, first key "i" will be the "version"
        # of the row, equivalent to the time spent so far in seconds.
        # "y" are all data rows of the current view.
        # with "i"+"y" and the position from meta we generate the columns in 
        # the order we defined in the meta definitions. this will be put into
        # a list and generates one row. after each iteration we put the stuff
        # to the rows list, which has all rows included.
        for i in data:
            for y in data[i]:
                row = []
                for position in column_order:
                    content = data[i][y][position]
                    
                    row.append((meta[position]['width'], SelectableText(str(content)+" ", meta[position]['align'], meta[position]['wrap'])))
                rows.append(urwid.AttrMap(urwid.Columns(row), 'body', 'focus'))
        return rows


    def basic_header(self):
        if DEBUG: keep("View().basic_header()")
        pass

    def basic_body(self):
        """ Simply return the body content """
        if DEBUG: keep("View().basic_body()")
        self.row()
        l = urwid.ListBox(
                urwid.SimpleListWalker(
                    self.row()
                        )
            )

        return l

    def basic_footer(self):
        if DEBUG: keep("View().basic_footer()")
        button_modes = ['Button 1', 'Button 2', "Quit"]
        footer_buttons = []
        for mode in button_modes:
            footer_buttons.append(self.button(mode, self.on_button_click))
        fcols = urwid.Columns(footer_buttons)
        return fcols
        #'return footer_buttons

    def basic_frame(self):
        if DEBUG: keep("View().frame()")
        #keep(self.text.get_text())
        
        f = urwid.Frame(
                urwid.AttrMap(
                    self.basic_body(), 'body'),
                #footer = urwid.AttrMap(
                #    self.basic_footer(), 'body'),
            )
        #keep(self.basic_footer())
        #self.controller.update()
        return f
    
    def main_window(self):
        if DEBUG: keep("View().main_window()")
        self.frame = self.basic_frame()
        return self.frame 

class Controller(object):
    """
    In a MVC pattern, the Controller class acts as a "bridge" from 
    Model() to View(). Sometimes the View() gets also the Controller() as 
    object but not in all cases.
    """
    def __init__(self):
        if DEBUG: keep("Controller().__init__()")

        self.update_alarm = None
        self.model = Model() # Initialise the Model

        # use the first mode as the default
        mode = self.get_modes()[0]
        self.model.set_mode(mode)

        self.view = View(self) # Initiate the View, add controller object

        self.set_redraw_window(True)
        #self.view.update()
    
    def set_redraw_window(self, state):
        """ If true it will redraw _everything_ you will lose focus"""
        if DEBUG: keep("Controller().set_redraw_screen(%s)" % state)
        self.view.set_redraw_window(state)

    def get_modes(self):
        """ Pipe list of modes from Model() to View() """
        if DEBUG: keep("Controller().get_modes()")
        return self.model.get_modes()

    def get_mode(self):
        """ Pipe current active mode from Model() to View() """
        if DEBUG: keep("Controller().get_modes()")
        return self.model.get_mode()

    def set_mode(self, m):
        """ Pipe mode from View() to Model() """
        if DEBUG: keep("Controller().set_mode()")
        self.model.set_mode(m)

    def get_data(self):
        """ 
        Provide data for the view, we get it from model class.
        As you can see, the controller class we are currently
        in acts like a "bridge" to provide the data.
        """
        if DEBUG: keep("Controller().get_data()")
        return self.model.get_data()
    
    def get_meta(self):
        """ 
        We also need the meta data, so that the View is capable
        of aranging every column correctly
        """
        if DEBUG: keep("Controller().get_meta()")
        return self.model.get_meta()

    def main(self):
        """ Start the MainLoop """
        if DEBUG: keep("Controller().main()")
        self.loop = urwid.MainLoop(self.view, self.view.palette, unhandled_input=self.keypress)
        self.update()
        self.loop.run()

    def update(self, loop=None, user_data=None):
        """ For automatic refreshing of the UI we need to set an alarm """
        if DEBUG: keep("Controller().update()")
        self.view.update()
        self.update_alarm = self.loop.set_alarm_in(UPDATE_INTERVAL, 
                self.update)
    
    def keypress(self, key):
        if DEBUG: keep("Controller().keypress()")
        if key == 'up':
            keep("UP")
        if key == 'down':
            keep("DOWN")
        if key == 'right':
            keep("RIGHT")
        if key == 'enter':
            keep("ENTER ENTER")
            #TODO Don't do direct call
            self.view.get_current_focus()

def keep(a):
    ''' A little debug function '''
    with open("/tmp/main.log", "a") as f:
        f.write(str(a)+"\n")

def main():
    if DEBUG: keep("\n\n\n####\nmain()")
    Controller().main()

if __name__ == '__main__':
    main()
