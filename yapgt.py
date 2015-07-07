#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import urwid
import psycopg2
import argparse

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
       
        self.modes = ['seq_idx', 'ins_upd_del', 'table_idx', 'table_io']
        # Connection object
        self.pg_conn = self.pg_connect()

    def set_mode(self, m):
        """ Set mode """
        if DEBUG: keep("Model().set_mode()")
        self.current_mode = m

    def get_modes(self):
        """ Return all modes that are defined in the Model()"""
        if DEBUG: keep("Model().get_modes()")
        return self.modes

    def get_data(self):
        if DEBUG: keep("Model().get_data()")
        return str(time.time())

    def pg_connect(self,
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database):
        """
        Small connection class
        If no password is suplied we try the default postgres user
        and expect a setted pg_ident or something similar
        """
        if DEBUG: keep("Model().pg_connect()")
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

    def pg_get_data(self):
        pass

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
        self.text = urwid.Text('a', 'left', 'clip')
        # As we need a initial starting point
        urwid.WidgetWrap.__init__(self, self.main_window())

    def update(self):
        """ Add new data to the views if an update occured """
        if DEBUG: keep("View().update()")
        self.text.set_text(self.controller.get_data())
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

    def basic_header(self):
        if DEBUG: keep("View().basic_header()")
        pass

    def basic_body(self):
        """ Simply return the body content """
        if DEBUG: keep("View().basic_body()")
        l = urwid.ListBox(
                urwid.SimpleListWalker(
                    [
                    urwid.AttrMap(
                        self.text, 'body'),
                    urwid.AttrMap(
                        urwid.Text("Row 2", 'left', 'clip'), 'body'),
                ])
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
        keep(self.text.get_text())
        
        f = urwid.Frame(
                urwid.AttrMap(
                    self.basic_body(), 'body'),
                footer = urwid.AttrMap(
                    self.basic_footer(), 'body'),
            )
        keep(self.basic_footer())
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
        self.view = View(self) # Initiate the View, add controller object

        # use the first mode as the default
        mode = self.get_modes()[0]
        self.model.set_mode(mode)


        self.view.update()

    def get_modes(self):
        """ Pipe list of modes from Model() to View() """
        if DEBUG: keep("Controller().get_modes()")
        return self.model.get_modes()

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
