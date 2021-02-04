#!/usr/bin/env python
# Tkinter gui project

"""This module goes & fetches fresh loans from a web server.
There is a URL that gives unprocessed XML file UUIDs one at a time.
Once the XML file is downloaded it is placed in the import
directory.  The Proloan monitoring process then picks
up the file & processes it.  If the process is successful
then a folder with the XML file prefix name is created.
When successful a sub folder XML is created.  On failure
the folder is called ERROR.  The ERROR folder contains
a log file.  This process will send feedback to the
web server every time it runs."""
import errno
import getpass
import http.client
import json
import logging
import shelve
import socket
import subprocess
import tempfile
import time
import tkinter.filedialog
import tkinter.messagebox
import tkinter.simpledialog
import tkinter.ttk  # For progress bar
import urllib.request, urllib.error, urllib.parse
import zipfile
import string
import os
import requests

from datetime import datetime, timedelta
from io import StringIO, BytesIO
from tkinter import *
from logging.handlers import TimedRotatingFileHandler
from shutil import copytree, rmtree


try:
    def ensure_dir_exists(path):
        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        except Exception as e:
            raise

    VERSION = '2.1.3'
    APPLICATION_NAME = 'Mobiloan XML Creator (%s)' % VERSION
    APPLICATION_DIR_NAME = 'Mobiloan XML Creator'
    PHRASE = 'cde'

    TEMP_DIR = os.path.join(os.getenv('APPDATA'), 'mobiloan_xml_creator')

    OLD_TEMP_DIR = os.path.join(tempfile.gettempdir(), 'mobiloan_xml_creator')
    try:
        # If the old settings directory exists in the temp dir, copy it to the AppData dir and then delete it
        if os.path.exists(OLD_TEMP_DIR) and not os.path.exists(TEMP_DIR):
            copytree(OLD_TEMP_DIR, TEMP_DIR)
            rmtree(OLD_TEMP_DIR)
    except Exception as e:
        print(str(e))

    ensure_dir_exists(TEMP_DIR)

    SYSTEM_SETTINGS_DB = os.path.join(TEMP_DIR, 'mobiloan_sync_settings.json')

    LOG_LOCATION = os.path.join(TEMP_DIR, 'mobiloan_xml_creator.log')
    ICON_FILENAME = 'mobiloan.ico'

    # REMOVE OLD EXE FILES
    current_working_dir = os.getcwd()
    for root, dirs, files in os.walk(current_working_dir):
        for file_name in files:
            if 'Mobiloan XML Creator' in file_name:
                full_file_path = os.path.join(root, file_name)
                os.remove(full_file_path)

    # Set up logger
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    logger = logging.getLogger('mobiloan_logger')
    logger.setLevel(logging.DEBUG)
    hdlr = TimedRotatingFileHandler(LOG_LOCATION,
                                    when='midnight',
                                    interval=1,
                                    backupCount=7)
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)

    main_window_geometry = '500x140'
    display_loans_geometry = '600x280'

    status = ''
    checked_for_updates = False  # Start off false so that only checks for updates once per session
    compulsory_update_pending = False

    # GUI Functions

    def main_gui():
        """
        Sets up the main windows elements in Tkinter
        :return:
        """
        global app, status_label, status_content, next_action_label, next_action_content, \
            proceed_button, scrolled_text, progress_label, status_bar, import_errors
        app = Tk()
        # Hide window until it is set up
        app.withdraw()
        app.grid
        center(app)
        app.title(APPLICATION_NAME)
        app.protocol("WM_DELETE_WINDOW", on_close_main)
        app.resizable(False, False)
        app.grid_columnconfigure(1, weight=1)
        app.grid_rowconfigure(5, weight=1)
        # **** Menu ****
        menu = Menu(app)
        app.config(menu=menu)
        file_menu = Menu(menu, tearoff=0)
        menu.add_cascade(label='File', menu=file_menu)
        file_menu.add_command(label='Settings', command=settings_gui)
        file_menu.add_command(label='Open log file', command=open_log_file)
        file_menu.add_separator()
        file_menu.add_command(label='Exit', command=on_close_main)

        # **** Buttons/Labels ****
        status_label = Label(app, text="Status:", font="Arial 10 bold")
        status_label.grid(row=0, column=0, sticky=E)
        status_content = Label(app)
        status_content.grid(row=0, column=1, columnspan=2, sticky=W)
        next_action_label = Label(app, text="       Next Action:", font="Arial 10 bold")
        next_action_label.grid(row=2, column=0, sticky=E)
        next_action_content = Label(app)
        next_action_content.grid(row=2, column=1, columnspan=2, sticky=W)
        proceed_button = Button(app, text="Proceed", width=12, command=proceed)
        proceed_button.grid(row=2, column=3, sticky=W, padx=(8, 8))

        # **** Progress/Status Bar ****
        progress_label = Label(app)
        progress_label.grid(row=4, column=0, sticky=W)
        status_bar = Label(app, text='', bd=1, relief=SUNKEN, anchor=W)
        import_errors = check_proloan_import_status()
        if import_errors:
            email_button = Button(app, text="Notify Admin", width=12, command=send_email_button)
            email_button.grid(row=0, column=3, sticky=W, padx=(8, 8))
            status_content['text'] = " %s loans failed to import to Proloan." % len(import_errors)
        next_action_content['text'] = 'Query server for new loans.'


    def settings_gui(**args):
        global app, system_settings, settings, drop_location_content, monitor_location_content, \
        environment, environment_content, temp_environment
        # Set up window
        app.withdraw()
        get_settings()
        settings = Toplevel()
        settings.title(APPLICATION_NAME)
        settings.protocol("WM_DELETE_WINDOW", on_close_settings)
        center(settings, height=450, width=600)
        settings.resizable(FALSE, FALSE)
        settings.grid_columnconfigure(1, weight=1)
        settings.grid_rowconfigure(4, weight=1)

        # Set padding variables
        heading_padx = (2, 0)
        heading_pady = (8, 0)
        label_padx = (6, 0)
        label_pady = (0, 0)
        content_padx = (10, 0)
        content_pady = (0, 0)
        button_padx = (0, 8)
        button_pady = (1, 1)
        option_padx = (0, 6)
        option_pady = (1, 1)
        edit_button_width = 13
        option_menu_width = 10
        temp_environment = system_settings['ENVIRONMENT']
        row = 0

        # Set elements
        directory_setup_label = Label(settings, text='Directory Setup:', font="Arial 9 bold")
        directory_setup_label.grid(row=row, column=0, sticky=W, padx=heading_padx, pady=heading_pady)
        row += 1

        drop_location_label = Label(settings, text='Import XML Path:')
        drop_location_label.grid(row=row, column=0, sticky=W, padx=label_padx, pady=label_pady)
        drop_location_content = Label(settings, text=system_settings['DROP_LOCATION'], justify=LEFT)
        drop_location_content.grid(row=row, column=1, sticky=W, padx=content_padx, pady=content_pady)
        drop_location_edit = Button(settings, text='Edit', width=edit_button_width, command=set_drop_location)
        drop_location_edit.grid(row=row, column=2, sticky=E, padx=button_padx, pady=button_pady)
        row += 1

        monitor_location_label = Label(settings, text='Import Results Path:')
        monitor_location_label.grid(row=row, column=0, sticky=W, padx=label_padx, pady=label_pady)
        monitor_location_content = Label(settings, text=system_settings['MONITOR_LOCATION'], justify=LEFT)
        monitor_location_content.grid(row=row, column=1, sticky=W, padx=content_padx, pady=content_pady)
        monitor_location_edit = Button(settings, text='Edit', width=edit_button_width, command=set_monitor_location)
        monitor_location_edit.grid(row=row, column=2, sticky=E, padx=button_padx, pady=button_pady)
        row += 1

        environment = StringVar(settings)
        environment_options = ['Testing', 'Staging', 'Production', 'Dev']
        option = OptionMenu(settings, environment, *environment_options)
        option.config(width=option_menu_width)
        environment.set(system_settings['ENVIRONMENT'])
        environment_label = Label(settings, text='Environment:')
        environment_label.grid(row=row, column=0, sticky=W, padx=label_padx, pady=label_pady)
        environment_content = Label(settings, text=system_settings['ENVIRONMENT'], wraplength=250, justify=LEFT)
        environment_content.grid(row=row, column=1, sticky=W, padx=content_padx, pady=content_pady)
        option.grid(row=row, column=2, sticky=E, padx=option_padx, pady=option_pady)
        environment.trace('w', set_environment)
        row += 1

        clear_button = Button(settings, text='Clear Old Files', font='Arial 9 bold', width=edit_button_width, command=clear_cache)
        clear_button.grid(row=row, column=0, sticky=W + S, padx=label_padx, pady=(8, 8))
        save_button = Button(settings, text='Save', font='Arial 9 bold', width=edit_button_width, command=save_and_exit_settings)
        save_button.grid(row=row, column=2, sticky=E + S, padx=button_padx, pady=(8, 8))

        # Initialise window
        settings.mainloop()


    def center(app, height=None, width=None):
        """
        Center window on the screen
        :param app:
        :return:
        """
        app.update_idletasks()

        w = app.winfo_width()
        h = app.winfo_height()

        if height or width:
            x = int(app.winfo_screenwidth() // 2) - (width // 2)
            y = int(app.winfo_screenheight() // 2) - (height // 2)
            app.geometry("%sx%s+%s+%s" % (width, height, x, y))
        else:
            x = int(app.winfo_screenwidth() // 2) - (w // 2)
            y = int(app.winfo_screenheight() // 2) - (h // 2)
            app.geometry("%s+%s+%s" % (main_window_geometry, x, y))


    def update_window():
        """
        Clears the progress labels and resets the status bar in the main window
        :return:
        """
        progress_label['text'] = ''
        status_bar.grid(row=5, column=0, columnspan=4, sticky=W + E)
        app.update()


    # UTILITY FUNCTIONS

    def set_icon():
        """
        Set the application icon
        :return:
        """
        try:
            app.iconbitmap(default=resource_path(ICON_FILENAME))
        except Exception:
            logger.error("Couldn't load icon")


    def resource_path(relative_path):
        """ Get absolute path to resource, works for dev and for PyInstaller """
        try:
            # PyInstaller creates a temp folder and stores path in _MEIPASS
            base_path = sys._MEIPASS
        except Exception as e:
            logger.error('resource_path:: ' + str(e))
            base_path = os.path.abspath(".")

        return os.path.join(base_path, relative_path)


    # HELPER FUNCTIONS

    def on_close_main():
        """
        Runs on closing the main window
        :return:
        """
        if status == 'busy':
            if tkinter.messagebox.askyesno("Exit Application", "Application running!\nExit now?"):
                sys.exit()
        else:
            sys.exit()


    def on_close_settings():
        """
        Runs on closing the settings window
        :return:
        """
        global app, settings

        # if get_settings() == 'incomplete':
        #     sys.exit()
        # else:
        settings.destroy()
        app.deiconify()


    def pl(num):
        """
        Returns an s if the input is greater than 1
        :param num:
        :return:
        """
        if num > 1:
            return 's'
        else:
            return ''


    def set_default_settings():
        default_settings = [('DROP_LOCATION', 'Downloaded XMLs'), ('MONITOR_LOCATION', 'Processed XMLs')]

        for setting_pair in default_settings:
            if setting_pair[0] not in list(system_settings.keys()) or system_settings[setting_pair[0]] == '':
                logger.info('No drop location set - attempting to set default')
                try:
                    # Check if exists
                    base_dir = os.environ["ProgramFiles(x86)"]
                except Exception as e:
                    logger.error(str(e))
                    # If ProgramFiles(x86) doesn't exist try ProgramFiles
                    base_dir = os.environ["ProgramFiles"]
                try:
                    temp_email = system_settings['ADMIN_EMAIL']
                except:
                    system_settings['ADMIN_EMAIL'] = 'admin@modalityapps.com'

                app_dir = os.path.join(base_dir, APPLICATION_DIR_NAME)
                if os.path.isdir(app_dir):
                    system_settings[setting_pair[0]] = os.path.join(base_dir, APPLICATION_DIR_NAME, setting_pair[1])


    def get_settings():
        """
        Fetch system settings from db and return if successful or not.
        :return:
        """
        global system_settings, temp_system_settings
        try:
            f = open(SYSTEM_SETTINGS_DB, 'r')
            system_settings = json.load(f)
            f.close()
        except Exception:
            with open(SYSTEM_SETTINGS_DB, 'w') as f:
                system_settings = {'BRANCHES':[{}]}
                json.dump({'BRANCHES':[{}]}, f)
        status = 'complete'
        temp_system_settings = system_settings
        required_settings = ('DROP_LOCATION', 'MONITOR_LOCATION', 'COMPANY_NAME', 'ENVIRONMENT', 'ADMIN_EMAIL')
        for name in required_settings:
            if name not in list(system_settings.keys()) or system_settings[name] == '':
                if name == 'ADMIN_EMAIL':  # Default email to errors to
                    system_settings[name] = 'admin@modalityapps.com'
                else:
                    system_settings[name] = ''
                    status = 'incomplete'
        if len(system_settings['BRANCHES']) > 0:
            for branch in range(len(system_settings['BRANCHES'])):
                required_settings = ('BRANCH_NAME', 'BRANCH_KEY', 'BRANCH_ID', 'LAST_RUN')
                for name in required_settings:
                    if name not in list(system_settings['BRANCHES'][branch].keys()) or system_settings['BRANCHES'][branch][name] == '':
                        if name == 'LAST_RUN':  # if no last run, then make it todays time
                            temp = datetime.today() - timedelta(30)
                            system_settings['BRANCHES'][branch][name] = str(temp.strftime('%Y-%m-%d %H:%M:%S.%f'))
                        else:
                            system_settings['BRANCHES'][branch][name] = ''
                        status = 'incomplete'
        
        set_default_settings()
        set_urls(system_settings['ENVIRONMENT'])

        temp_system_settings = system_settings
        return status


    def set_drop_location():
        """
        Prompt user for a directory to save downloaded XMLs
        :return:
        """
        global settings, drop_location_content, system_settings
        setting = 'DROP_LOCATION'
        if system_settings[setting] == '' and system_settings['MONITOR_LOCATION'] != '':
            new_dir = tkinter.filedialog.askdirectory(initialdir=system_settings['MONITOR_LOCATION'])
        else:
            new_dir = tkinter.filedialog.askdirectory(initialdir=system_settings[setting])

        if new_dir != '':
            drop_location_content['text'] = str(new_dir)
            system_settings[setting] = new_dir.replace('/', '\\')
        settings.update()


    def set_monitor_location():
        """
        Prompt user for directory of processed XMLs
        :return:
        """
        global settings, monitor_location_content, system_settings
        setting = 'MONITOR_LOCATION'
        if system_settings[setting] == '' and system_settings['DROP_LOCATION'] != '':
            new_dir = tkinter.filedialog.askdirectory(initialdir=system_settings['DROP_LOCATION'])
        else:
            new_dir = tkinter.filedialog.askdirectory(initialdir=system_settings[setting])
        if new_dir != '':
            monitor_location_content['text'] = str(new_dir)
            system_settings[setting] = new_dir.replace('/', '\\')
        settings.update()


    def set_environment(*args, **kwargs):
        global environment_content, settings, temp_environment
        try:
            env = environment.get()
        except Exception as e:
            env = args[0]
        temp_environment = env
        try:
            environment_content['text'] = temp_environment
            settings.update()
        except Exception as e:
            pass


    def save_and_exit_settings():
        global temp_environment, settings_json
        # Ensure branch name is not empty
        if system_settings['DROP_LOCATION'] == '':
            tkinter.messagebox.showinfo('Missing Info', 'Please select an Import XML Path')
            return
        if system_settings['MONITOR_LOCATION'] == '':
            tkinter.messagebox.showinfo('Missing Info', 'Please select an Import Results Path')
            return

        # Check if branch name returned matches branch name entered
        if temp_environment:
            system_settings['ENVIRONMENT'] = temp_environment
            set_urls(temp_environment)

            loans = None
            status = 'nothing'
            with open(SYSTEM_SETTINGS_DB, 'w') as f:
                json.dump(system_settings, f)
            if tkinter.messagebox.askokcancel("Configuration Successful", "Save and exit?"):
                on_close_settings()

        else:
            tkinter.messagebox.showinfo('Invalid Credentials', "Please try again.")


    def set_urls(environment):
        global URL, GET_INSTANCE_URL,\
            GET_ZIP, SEND_EMAIL, CHECK_FOR_UPDATES, DOWNLOAD_NEW_VERSION, system_settings

        if environment == 'Dev':
            BASE_URL = 'http://127.0.0.1:8000'  # For testing on local server
        elif environment == 'Production':
            BASE_URL = 'https://api.modalityapps.com'
        else:
            BASE_URL = 'http://api-%s.modalityapps.com' % environment.lower()
                

        # For local development use local server (based on Windows username)
        GET_INSTANCE_URL = BASE_URL + '/get_branch_details'
        GET_ZIP = BASE_URL + '/get_xmls_zipped'
        SEND_EMAIL = BASE_URL + '/send_email'
        CHECK_FOR_UPDATES = BASE_URL + '/check_for_updates?version=%s' % VERSION
        DOWNLOAD_NEW_VERSION = BASE_URL + '/download_xml_creator'


    def check_settings():
        if get_settings() == 'incomplete':
            tkinter.messagebox.showinfo('Settings Required', 'Please complete settings before continuing')
            settings_gui()


    def clear_cache():
        get_processed_ids()
        tkinter.messagebox.showinfo('Memory Cleared', 'Memory cleared successfully')

    # WEB SERVICES

    def open_log_file():
        global LOG_LOCATION
        subprocess.call(['notepad', LOG_LOCATION])


    def check_proloan_import_status():
        """
        Walk the directory to monitor & look for xml's that failed to import to Proloan
        @return: error id's (list)
        """
        errors = []
        for root, dirs, files in os.walk(system_settings['MONITOR_LOCATION']):
            if root.find('Error') > 0:
                for xml_file in files:
                    errors.append(xml_file.replace('.xml', ''))
        return errors

    def send_email_button():
        if import_errors:
            if tkinter.messagebox.askyesno("Errors found", "Send an email to: %s?" % system_settings['ADMIN_EMAIL']):
                subject = "Proloan failed imports for %s" % system_settings['COMPANY_NAME']
                send_email(subject, "<br>".join(import_errors))

    def send_email(subject, body):
        if not system_settings['ADMIN_EMAIL']:
            system_settings['ADMIN_EMAIL'] = 'admin@modalityapps.com'
        json_data = {
            "to_address": system_settings['ADMIN_EMAIL'],
            "cc_addresses": [],
            "bcc_addresses": [],
            "from_address": "no-reply@modalityapps.com",
            "subject": subject,
            "content": body
        }
        response = requests.post(SEND_EMAIL,
                                 headers={'content-type': 'application/json',
                                          'Authorization': system_settings['SERVER_AUTH']},
                                 data=json.dumps(json_data))


    # Main Functions

    def proceed():
        """
        Queries the server for loans pending import into proloan
        :return:
        """
        global logger, answer, d, data, loans, loan_id, xml_file, contents, fn, ffn, file, status, scrolled_text, checked_for_updates, compulsory_update_pending
        check_settings()

        if not checked_for_updates or compulsory_update_pending:
            result = check_for_updates()
            if result:
                checked_for_updates = True
            else:
                return
        # First check if there are any errors to report
        status_content['text'] = 'Busy...'
        app.update()
        proceed_button.config(state='disabled')
        try:
            scrolled_text.grid_remove()
            app.geometry(main_window_geometry)
            app.update()
        except NameError:
            pass

        # Create progress bar
        pb = tkinter.ttk.Progressbar(app, mode='indeterminate')
        # Hide status bar
        status_bar.grid_remove()
        # Show progress bar
        pb.grid(row=5, column=0, columnspan=4, sticky=EW)
        pb.start()
        app.update()
        try:
            proceed_button.config(state='normal')
            app.update()
            check_settings()
            fetch_xmls()
            update_window()
        except urllib.error.HTTPError as e:
            logger.error('HTTP Error. ' + str(e))
            tkinter.messagebox.showerror('Error', 'Sorry, the following HTTP error was encountered:\n' + str(e))
        except (urllib.error.URLError, socket.timeout) as e:
            logger.error('URLError. ' + str(e))
            tkinter.messagebox.showerror('Error', 'Sorry, a URL error was encountered. \n'
                                            'Please check your INTERNET CONNECTION and if the problem persists contact '
                                            'the developer(1).\n\n'
                                            'Error: ' + str(e))
        except http.client.HTTPException as e:
            logger.error('HTTPException. ' + str(e))
            tkinter.messagebox.showerror('Error', 'Sorry, the following HTTPException error was encountered:\n' + str(e))
        except AssertionError:
            logger.error('Unauthorized')
            tkinter.messagebox.showerror('Error', 'Sorry, request unauthorized. Please contact your system administrator')
        except Exception as e:
            tkinter.messagebox.showerror('Error', 'Sorry, the following error was encountered:\n' + str(e))
            logger.info(str(e))
        finally:
            progress_label['text'] = ''
            pb.stop()
            pb.grid_remove()
            status_bar.grid(row=5, column=0, columnspan=4, sticky=W + E)
            app.update()
            update_window()


    def fetch_xmls():
        """
        Loops through the loans array passing the loan ids to the server and retrieving the corresponding xml.
        Loans are marked as got_file in the system_status db if successful else marked as xml_download_error.
        :return:
        """
        global loans, xml_file, settings, contents, fn, ffn, file, status, system_settings, proceed_button
        status = 'busy'
        # Create progress bar
        pb = tkinter.ttk.Progressbar(app, mode='determinate')
        progress = 0
        pb.config(maximum=3, variable=progress)
        app.geometry(main_window_geometry)
        status_bar.grid_remove()
        status_content['text'] = 'Fetching loans on server...'
        next_action_content['text'] = ''
        proceed_button.config(state='disabled')
        progress_label.grid(row=4, column=0, sticky=W)
        pb.grid(row=5, column=0, columnspan=4, sticky=EW)
        app.update()
        success_count = 0
        creation_error_count = 0
        general_error_count = 0
        connection_error = False
        server_error = False
        logger.info("*" * 30 + " Fetching new loans from server " + "*" * 30)
        processed_ids = get_processed_ids()
        processed_loan_data = {}
        failed_ids = []
        for k in range(len(system_settings['BRANCHES'])):
            branch_id = system_settings['BRANCHES'][k]['BRANCH_ID']
            try:
                response_data = {
                    'branch_id': branch_id
                }
                url = '%s?branch_id=%s&starting_date=%s' % (GET_ZIP, branch_id, system_settings['BRANCHES'][k]['LAST_RUN'])
                response = requests.post(url,
                                        headers={'content-type': 'application/json',
                                                'Authorization': system_settings['SERVER_AUTH']},
                                        data=json.dumps(response_data),
                                        timeout=100)
                if response.status_code == 403:
                    response = json.loads(response.content)
                    tkinter.messagebox.showerror(response['title'], response['message'])
                    status = 'found'
                    pb.grid_remove()
                    return

                zipdata = BytesIO()
                zipdata.write(response.content)
                myzipfile = zipfile.ZipFile(zipdata)
            # TODO stop code from running further if "myzipfile" was not initialized
            except urllib.error.HTTPError as e:
                logger.error('HTTP Error. ' + str(e))
                tkinter.messagebox.showerror('Error', 'Sorry, the following HTTP error was encountered:\n' + str(e))
                continue
            except requests.ConnectionError as e:
                logger.error('Connection error fetching zip file: ' + str(e))
                connection_error = True
            except Exception as e:
                logger.error('Error fetching zip file: ' + str(e))
                connection_error = True
                continue

            status_content['text'] = 'Downloading loans...'
            pb.step(1)
            app.update()

            loans = myzipfile.namelist()
            for fn in loans:
                if fn in processed_ids:
                    logger.info('Xml has already been imported: ' + fn)
                    continue
                try:
                    if fn == 'last_date':
                        last_date = myzipfile.open(fn).read()
                        if last_date:
                            system_settings['BRANCHES'][k]['LAST_RUN'] = last_date.decode("utf-8") 
                        continue
                    loan_id = fn[:-4]
                    # Get xml file from zip
                    xml_file = myzipfile.open(fn)
                    # Read xml file
                    contents = xml_file.read()
                    # Write xml file locally
                    ffn = os.path.join(system_settings['DROP_LOCATION'], fn)
                    xml_file = open(ffn, 'wb')
                    xml_file.write(contents)
                    xml_file.close()
                    success_count += 1
                    processed_loan_data[loan_id] = 'success'
                except KeyError as e:
                    logger.error(str(e))
                    processed_loan_data[loan_id] = 'xml_download_error'
                    failed_ids.append(loan_id)
                    creation_error_count += 1
                except Exception as e:
                    logger.error('Error fetching xml from zipfile: %s' % loan_id)
                    logger.error(str(type(e)) + ' : ' + str(e))
                    failed_ids.append(loan_id)
                    general_error_count += 1

        logs = None
        if general_error_count:
            with open(LOG_LOCATION) as f:
                logs = f.read()

        pb.step(1)
        app.update()

        pb.step(1)
        status = 'nothing'
        app.update()
        with open(SYSTEM_SETTINGS_DB, 'w') as f:
            json.dump(system_settings, f)

        done_text = 'Done. %s XML%s downloaded.' % (success_count, pl(success_count))
        logger.info("-" * 20 + "> %s XML%s successfully downloaded." %
                    (success_count, pl(success_count)))
        logger.info("-" * 20 + "> %s XML download%s failed." % (creation_error_count, pl(creation_error_count)))
        if failed_ids:
            logger.info('Failed loans: ' + str(failed_ids))
            subject = "XML download failed for company: " + system_settings['COMPANY_NAME']
            send_email(subject, "<br>".join(failed_ids))
        if creation_error_count > 0:
            done_text += ' %s failed.' % creation_error_count
        proceed_button.config(state='normal')
        status_content['text'] = done_text
        status_bar['text'] = done_text
        pb.grid_remove()
        status_bar.grid(row=5, column=0, columnspan=4, sticky=W + E)
        app.update()
        if connection_error:
            tkinter.messagebox.showerror('Error', 'Sorry, a URL error was encountered while fetching loans. \n'
                                            'Please check your INTERNET CONNECTION and if the problem persists contact '
                                            'the developer.(2)')
        if server_error:
            tkinter.messagebox.showerror('Error', 'A server error was encountered while processing. \n'
                                            'Please try again and if the problem persists contact '
                                            'the developer.(2)')


    def clean_up_files():
        """
        Walk through monitor location and delete all files older than X seconds and all empty directories
        :param base_dir:
        :return:
        """
        try:
            base_dir = system_settings['MONITOR_LOCATION']
            file_array = []
            for root, dirs, files in os.walk(base_dir):
                if ('Xml Error Files' in dirs) and root != base_dir:
                    if len(files) > 0:
                        dirs.remove('Xml Error Files')
                    for xml_dir in dirs:
                        path = os.path.join(root, xml_dir)
                        for f in os.listdir(path):
                            file_path = os.path.join(path, f)
                            os.remove(file_path)
                        os.rmdir(path)
                    try:
                        os.rmdir(root)
                    except:
                        continue
        except Exception as e:
            print(str(e))


    def get_processed_ids():
        """
        Walk through monitor location and add all successful loan ids to an array
        :param:
        :return:
        """
        try:
            base_dir = system_settings['MONITOR_LOCATION']
            file_array = []
            for root, dirs, files in os.walk(base_dir):
                if ('Xml' in dirs) and root != base_dir:
                    path = os.path.join(root, 'Xml')
                    for f in os.listdir(path):
                        file_array.append(f)
            return file_array
        except Exception as e:
            logger.error(str(e))

    # Setup
    def download_new_version():
        global DOWNLOAD_NEW_VERSION, compulsory_update_pending
        status_content['text'] = 'Downloading update...'
        next_action_content['text'] = ''

        # Create progress bar
        pb = tkinter.ttk.Progressbar(app, mode='determinate')
        # Hide status bar
        status_bar.grid_remove()
        # Show progress bar
        pb.grid(row=5, column=0, columnspan=4, sticky=EW)

        app.update()
        root_dir = tempfile.gettempdir()
        local_filename = os.path.join(root_dir, 'mobiloan_xml_creator_update.exe')
        # Delete old update file if it exists
        if os.path.isfile(local_filename):
            os.remove(local_filename)

        try:
            file_name = local_filename
            res = requests.get(DOWNLOAD_NEW_VERSION, headers={'Authorization': system_settings['SERVER_AUTH']}, timeout=30)
            u = res.content
            f = open(file_name, 'wb')
            logger.info('Download update started..')
            f.write(u)
            pb.step(8192)
            app.update()
            f.close()
            logger.info('Download update complete.')
            tkinter.messagebox.showinfo('Update ready', "Please follow installation instructions")
            # Launch setup and then kill this thread
            subprocess.Popen([local_filename])
            sys.exit(0)
        except urllib.error.HTTPError as e:
            logger.error('Error downloading update: ' + str(e))
            tkinter.messagebox.showerror('Error', 'Error downloading update')
        except (urllib.error.URLError, socket.timeout) as e:
            logger.error('URL (connection) error downloading update: ' + str(e))
            tkinter.messagebox.showerror('Connection error', 'Connection error. Please check your internet connection')
        except Exception as e:
            logger.error('Error downloading update: ' + str(e))
            tkinter.messagebox.showerror('Error', 'Error downloading update\n\n' + str(e))


    def check_for_updates():
        global CHECK_FOR_UPDATES, compulsory_update_pending

        try:
            response = requests.get(CHECK_FOR_UPDATES,
                                    headers={'Authorization': system_settings['SERVER_AUTH']})
            response_data = json.loads(response.content)
        except Exception as e:
            logger.error(str(e))
            tkinter.messagebox.showerror('Error', 'Error checking for updates')
            #return

        if response.status_code == 200:
            logger.info('New version available: ' + response_data['version'])
            if not response_data['compulsory']:
                ans = tkinter.messagebox.askokcancel(title='New update available',
                                               message='There is a new update available. Would you like to download now?')
                if ans:
                    download_new_version()
                return True
            else:
                compulsory_update_pending = True
                ans = tkinter.messagebox.askokcancel(title='Compulsory update',
                                               message='There is a new compulsory update. Download now?')
                if ans:
                    download_new_version()
                else:  # If user chooses not to download compulsory update - exit
                    sys.exit(0)
                return True

        elif response.status_code == 500:
            logger.error(response_data['reply_str'])
            tkinter.messagebox.showerror('Error', 'Error checking for updates')

        elif response.status_code == 404:
            logger.info('Already on latest version')
            return True

        elif response.status_code == 204:
            return True

    get_settings()
    main_gui()
    set_icon()
    set_urls(system_settings['ENVIRONMENT'])
    center(app)
    update_window()
    app.deiconify()

    # check is another instance of same program running
    tempdir = tempfile.gettempdir()
    lockfile = os.sep.join([tempdir, 'myapp.lock'])
    try:
        if os.path.isfile(lockfile):
            os.unlink(lockfile)
    except WindowsError as e: # Should give you smth like 'WindowsError: [Error 32] The process cannot access the file because it is being used by another process..'   
        # there's instance already running
        tkinter.messagebox.showinfo('Already Running', APPLICATION_NAME + ' is already running!')
        sys.exit(-1)

    with open(lockfile, 'wb') as lockfileobj:
        # run your app's main here
        app.mainloop()
except Exception as e:
    print((str(e)))
    logger.error(str(e))
