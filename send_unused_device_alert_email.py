import logging
 from datetime import date, timedelta, datetime
 
 from dateutil import parser
 from django.conf import settings
 from django.core.management.base import BaseCommand
 from django.template import loader
 
 from bs4 import BeautifulSoup
 from red_fin import tasks
 from red_fin.models import MobiloanInstance, MobiloanBranch
 from red_fin.utils import is_workday
 from syncchron.journey import JourneyCompany, JourneyWorker, JourneyBranch
 
 logger = logging.getLogger('cron')
 
 
 class Command(BaseCommand):
     def handle(self, *args, **options):
         """
         :param args:
         :param options:
         :return:
         """
 
         # Only continue if today is work day
         try:
             if not is_workday():
                 return
         except Exception as e:
             logger.error('Unable to check if workday')
 
         logger.info("*" * 30 + " Starting unused_device_alert_email" + "*" * 30)
         mobiloan_instances = MobiloanInstance.objects.all()
         for mobiloan_instance in mobiloan_instances:
             companies = mobiloan_instance.company_set.all()
             for company in companies:
                 try:
                     # UNUSED DEVICES FOR COMPANY
                     # Get Journey Company
                     journey_company = JourneyCompany(api_user=mobiloan_instance.api_username,
                                                      api_password=mobiloan_instance.api_password)
                     journey_company.fetch(object_id=company.journey_id)
                     if journey_company.device_not_connected_warning_days:
                         period = journey_company.device_not_connected_warning_days
                     else:
                         break
                     boundary_date = date.today() - timedelta(days=period)
 
                     # Query for devices
                     device_instance = JourneyWorker(api_user=mobiloan_instance.api_username,
                                                     api_password=mobiloan_instance.api_password)
                     devices = device_instance.query([('q', 'company_id', journey_company.id),
                                                      ('q', '_updated_at.lt', boundary_date),
                                                      ('q', 'status', 0),
                                                      ('sort', 'name', 'asc')])
 
                     # Parse dates on devices
                     for device in devices:
                         if device.date_login is not None:
                             device.date_login = parser.parse(device.date_login)
                         if device.enrollment['last_connected'] is not None:
                             device.enrollment['last_connected'] = parser.parse(device.enrollment['last_connected'])
 
                     if devices:
                         logger.info('Found %s unused devices for %s ' % (len(devices), company.trading_name))
                         # Populate and email the template the the branch email address
                         data = {
                             'now': datetime.now(),
                             'environment': settings.ENVIRONMENT,
                             'devices': devices,
                             'company': company
                         }
                         try:
                             email_template = loader.get_template('email_templates/unused_device_alert_email.html')
                             email_html = email_template.render(data)
                         except Exception as e:
                             error_msg = 'Error loading email template: ' + str(e)
                             logger.error(error_msg)
                             self.stdout.write('Error')
                             raise e
 
                         # Email
                         try:
                             # Parse html content to prevent lines being too long (mail server limit is 990 char)
                             soup = BeautifulSoup(email_html, features="html5lib")
                             content = soup.prettify()
                             subject = 'Mobiloan - %s - Unused Devices Alert' % journey_company.trading_name
                             tasks.email(
                                 content=content,
                                 subject=subject,
                                 to_address=journey_company.email_address,
                                 from_address='no-reply@modalityapps.com'
                             )
                         except Exception as e:
                             error_msg = "Error sending email. " + str(e)
                             logger.error(error_msg)
                             self.stdout.write('Error')
                             raise e
 
                     # UNUSED DEVICES FOR BRANCHES
                     branches = MobiloanBranch.objects.filter(company=company)
                     for branch in branches:
                         # Query for devices
                         device_instance = JourneyWorker(api_user=mobiloan_instance.api_username,
                                                         api_password=mobiloan_instance.api_password)
                         devices = device_instance.query([('q', 'branch_id', branch.journey_id),
                                                          ('q', '_updated_at.lt', boundary_date)])
 
                         if devices:
 
                             # Parse dates on devices
                             for device in devices:
                                 device.updated_at = parser.parse(device._updated_at)
                                 if device.enrollment['last_connected'] is not None:
                                     device.enrollment['last_connected'] = parser.parse(device.enrollment['last_connected'])
 
                             # Get Journey Branch
                             journey_branch = JourneyBranch(api_user=mobiloan_instance.api_username,
                                                            api_password=mobiloan_instance.api_password)
                             journey_branch.fetch(object_id=branch.journey_id)
 
                             logger.info('Found %s unused devices for %s ' % (len(devices), company.trading_name))
                             # Populate and email the template the the branch email address
                             data = {
                                 'now': datetime.now(),
                                 'environment': settings.ENVIRONMENT,
                                 'devices': devices,
                                 'company': company,
                                 'branch': branch
                             }
                             try:
                                 email_template = loader.get_template('email_templates/unused_device_alert_email.html')
                                 email_html = email_template.render(data)
                             except Exception as e:
                                 error_msg = 'Error loading email template: ' + str(e)
                                 logger.error(error_msg)
                                 self.stdout.write('Error')
                                 raise e
 
                             # Email
                             try:
                                 # Parse html content to prevent lines being too long (mail server limit is 990 char)
                                 soup = BeautifulSoup(email_html, features="html5lib")
                                 content = soup.prettify()
                                 subject = 'Mobiloan - %s - Unused Devices Alert' % branch.name
                                 tasks.email(
                                     content=content,
                                     subject=subject,
                                     to_address=journey_branch.email_address,
                                     from_address='no-reply@modalityapps.com'
                                 )
                             except Exception as e:
                                 error_msg = "Error sending email. " + str(e)
                                 logger.error(error_msg)
                                 self.stdout.write('Error')
                                 raise e
                 except Exception as e:
                     logger.error('Unused device error for ' + company.trading_name + ' :' + str(e))
                     continue
         logger.info("*" * 30 + " Finished unused_device_alert_email" + "*" * 30)