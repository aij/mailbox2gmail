#!/usr/bin/python

# mailbox2gmail: Import maildir mails to Gmail.
# Copyright (C) 2013 Ivan Jager
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import os, sys, time, stat, argparse, getpass
import mailbox
from gdata.apps.migration import service
import gdata.apps.service
import aij_threading


def is_maildir(path):
    for d in ['', 'new', 'cur', 'tmp']:
        if not os.path.isdir(os.path.join(path, d)):
            return False
    return True

def find(path):
    for f in os.listdir(path):
        f = os.path.join(path, f)
        yield f
        if os.path.isdir(f):
            for x in find(f):
                yield x

def find_maildirs(path):
    return (p for p in find(path) if is_maildir(p))


class Uploader(object):
    def __init__(self, email, password, username, domain, labels, num_threads=60, prefix=''):
        print 'email:', email, 'username:', username, 'domain:', domain, 'labels:', labels
        self.username = username
        self.domain = domain
        self.labels = labels
        self.prefix = prefix
        self.service = service.MigrationService(
            email=email,
            password=password,
            domain=domain,
            source='org.mrph.ivan.maildir2gmail.v1')
        self.service.ProgrammaticLogin()
        self.thread_pool = aij_threading.ThreadPool(num_threads)
        self.failures = []


    """Migrate an entire mailbox to gmail."""
    def migrate_mailbox(self, mailbox, extra_labels=[], extra_properties=[]):
        print '\nMigrating mailbox', extra_labels, extra_properties
        for m in mailbox:
            self.migrate_message(m, extra_labels, extra_properties)

    def migrate_maildirs(self, path, recurse=False):
        md = mailbox.Maildir(path, factory=None, create=False)
        lab = [self.prefix] if self.prefix else []
        self.migrate_mailbox(md, extra_labels=lab, extra_properties=['IS_INBOX'])
        if recurse:
            for p in find_maildirs(path):
                md = mailbox.Maildir(p, factory=None, create=False)
                lab = self.prefix + p[len(path):]
                self.migrate_mailbox(md, extra_labels=[lab])

    def migrate_message(self, message, extra_labels=[], extra_properties=[]):
        mail_item_properties = []+extra_properties
        flags = message.get_flags()
        if 'D' in flags:  # Draft
            mail_item_properties.append('IS_DRAFT')
        if 'F' in flags:  # Flagged
            mail_item_properties.append('IS_STARRED')
        if 'P' in flags:  # Passed (forwarded, resent, or bounced)
            pass  # Unsupported by gmail.
        if 'R' in flags:  # Replied
            pass  # Unsupported by gmail.
        if 'S' not in flags:  # not Seen
            mail_item_properties.append('IS_UNREAD')
        if 'T' in flags:  # Trashed
            mail_item_properties.append('IS_TRASH')

        # TODO: IS_SENT

        work = self.do_import_mail(
            mail_message=str(message),
            mail_item_properties=mail_item_properties,
            mail_labels=self.labels+extra_labels)
        self.thread_pool.run(work)

    def import_mail_or_fail(self, mail_message, mail_item_properties, mail_labels, retries):
        # Errors I've seen from ImportMail include:
        #   HTTPException(32, 'Broken pipe')
        #   BadStatusLine("''",)
        #   gaierror(-2, 'Name or service not known')
        #   SSLError(8, '_ssl.c:504: EOF occurred in violation of protocol')
        #   HTTPException(101, 'Network is unreachable')
        # and several AppsForYourDomainException exceptions, including:
        #  many invalid dates like AppsForYourDomainException({'status': 400, 'body': 'Invalid RFC 822 Message: Date header &quot;Wednesday, 21 January 2009 10:15:00 -0600 &quot; is invalid.', 'reason': 'Bad Request'},)
        #   AppsForYourDomainException({'status': 400, 'body': 'Permanent failure: Insert failed, badly formed message.', 'reason': 'Bad Request'},)
        #   AppsForYourDomainException({'status': 502, 'body': '<!DOCTYPE html>\n<html lang=en>\n  <meta charset=utf-8>\n  <meta name=viewport content="initial-scale=1, minimum-scale=1, width=device-width">\n  <title>Error 502 (Server Error)!!1</title>\n  <style>\n    *{margin:0;padding:0}html,code{font:15px/22px arial,sans-serif}html{background:#fff;color:#222;padding:15px}body{margin:7% auto 0;max-width:390px;min-height:180px;padding:30px 0 15px}* > body{background:url(//www.google.com/images/errors/robot.png) 100% 5px no-repeat;padding-right:205px}p{margin:11px 0 22px;overflow:hidden}ins{color:#777;text-decoration:none}a img{border:0}@media screen and (max-width:772px){body{background:none;margin-top:0;max-width:none;padding-right:0}}\n  </style>\n  <a href=//www.google.com/><img src=//www.google.com/images/errors/logo_sm.gif alt=Google></a>\n  <p><b>502.</b> <ins>That\xe2\x80\x99s an error.</ins>\n  <p>The server encountered a temporary error and could not complete your request.<p>Please try again in 30 seconds.  <ins>That\xe2\x80\x99s all we know.</ins>\n', 'reason': 'Bad Gateway'},)
        #   AppsForYourDomainException({'status': 400, 'body': 'Invalid RFC 822 Message: Missing required &quot;Date:&quot; header.', 'reason': 'Bad Request'},)
        #   AppsForYourDomainException({'status': 400, 'body': 'Permanent failure: BadAttachment', 'reason': 'Bad Request'},)
        # And a ton of 503's saying to retry in 30 seconds.
        # The 400s seems pretty permanent, but all the others should be retried.
        try:
            self.service.ImportMail(
                user_name=self.username,
                mail_message=mail_message,
                mail_item_properties=mail_item_properties,
                mail_labels=mail_labels)
            sys.stdout.write('.')
            sys.stdout.flush()
        except:
            e = sys.exc_info()[1]
            if (isinstance(e, gdata.apps.service.AppsForYourDomainException)
                and e[0]['status'] == 400) or not retries:
                sys.stdout.write('@')
                sys.stdout.flush()
                raise
            else:
                sys.stdout.write('!')
                sys.stdout.flush()
                time.sleep(30)
                self.import_mail(mail_message, mail_item_properties, mail_labels, retries-1)

    def import_mail(self, mail_message, mail_item_properties, mail_labels, retries):
        try:
            self.import_mail_or_fail(mail_message, mail_item_properties, mail_labels, retries)
        except:
            f =(mail_message, mail_item_properties, mail_labels, sys.exc_info()[1])
            self.failures.append(f)
            print repr(f)

    def do_import_mail(self, mail_message, mail_item_properties, mail_labels, retries=20):
        def curry():
            self.import_mail(mail_message, mail_item_properties, mail_labels, retries)
        return curry


    def retry_failures(self, failures):
        for (mail_message, mail_item_properties, mail_labels, e) in failures:
            work = self.do_import_mail(mail_message, mail_item_properties, mail_labels)
            self.thread_pool.run(work)


    def finish(self):
        if self.failures:
            print 'failures =', repr(self.failures)
            failures = self.failures
            print 'Trying those messages one last time. After this you get to deal with them yourself.'
            self.failures = []
            self.retry_failures(failures)
            print 'remaining_failures =', repr(self.failures)
        print 'Closing thread pool'
        self.thread_pool.close()
        print repr(self.failures)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Upload Maildir emails to Gmail.')
    parser.add_argument('--email', '-e', required=True, help='email address for authentication')
    parser.add_argument('--maildir', '--dir', '-m', required=True, help='maildir to upload')
    parser.add_argument('--label', action='append', help='labels to apply to uploaded messages')
    parser.add_argument('--domain', help='domain to upload mail to')
    parser.add_argument('--username', help='username to upload mail for')
    parser.add_argument('--prefix', help='extra prefix to add to labels')
    parser.add_argument('--recurse', '-R', action='store_true',
                        help='recursively look for more maildirs to upload')
    args = parser.parse_args()

    password = getpass.getpass('Password for %s: ' % args.email)
    username = args.username or args.email.split('@', 1)[0]
    domain = args.domain or args.email.split('@', 1)[1]
    uploader = Uploader(args.email, password, username,domain, args.label)
    uploader.migrate_maildirs(args.maildir, recurse=args.recurse)
    uploader.finish()
