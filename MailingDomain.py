__author__ = 'Xiangyu Ren'

import MySQLdb
import re
import datetime
import string
import time

class DatabaseConnector:
    # function: connect to database
    def __init__(self, host, user, pwd, db):
        # parameter: host,user,pwd,db, all string type
        # function: connect to db
        self.con = MySQLdb.connect(host, user, pwd, db)
        self.addTable()

    def addTable(self):
        # function: create tables( Table mailing: save all mailing address; Domain: save all domains, date, and the number of its daily count)
        with self.con:
            cur = self.con.cursor()
            try:
                cur.execute("CREATE TABLE IF NOT EXISTS mailing ( addr VARCHAR(255) NOT NULL)")
                cur.execute("CREATE TABLE IF NOT EXISTS domain( "
                            "ID INT(10) NOT NULL AUTO_INCREMENT,"
                            "addrdomain VARCHAR(255) NOT NULL, "
                            "date DATE NOT NULL, "
                            "count INT(10) NOT NULL,"
                            "PRIMARY KEY (ID) );")
                self.con.commit()
            except:
                self.con.rollback()



class MailingManagement:
    # function: update two tables
    def __init__(self):
        # function: connect to database
        self.db = DatabaseConnector('localhost', 'root', 'ren39338', 'mailinglist')

    def getDomain(self, addr):
        # parameter: addr, a string
        # function: get the domain of a mail address
        index = addr.find('@')
        return addr[(index+1):]

    def updateDomain(self, addrDomain, dt):
        # parameter: addrDomain, a string
        # function: update domain table
        if addrDomain != "":
            with self.db.con:
                try:
                    cur = self.db.con.cursor()
                    cur.execute("SELECT * FROM domain WHERE addrdomain = %s and date = %s", (addrDomain, dt))
                    searchresult = cur.fetchone()
                    if searchresult:
                       cur.execute("UPDATE domain SET count = count + 1 WHERE addrdomain = %s and date = %s", (addrDomain, dt))
                    else:
                       cur.execute("INSERT INTO domain (addrdomain, date, count) VALUES (%s, %s, 1)", (addrDomain, dt))
                    self.db.con.commit()
                except:
                    self.db.con.rollback()

    def insertMail(self,addr,date):
        # parameter: addr, a string
        # function: insert a new mail address to mailing table
        pattern = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"
        isMatch = re.match(pattern,addr)
        if isMatch:
            with self.db.con:
                cur = self.db.con.cursor()
                try:
                    cur.execute("SELECT * FROM mailing WHERE addr = %s", (addr))
                    searchresult = cur.fetchone()
                    if searchresult:
                        pass
                    else:
                        cur.execute("INSERT INTO mailing (addr) VALUES (%s)", (addr))
                    self.db.con.commit()
                    do = self.getDomain(addr)
                    self.updateDomain(do,date)
                    print('Successfully insert');
                except:
                    self.db.con.rollback()
        else:
            print("Invalid mail address")
        self.disconnectdatabase()

    def disconnectdatabase(self):
    # function: disconnect the database
        self.db.con.close()




class MatchDomain:
# function: a node consists of domain and its growth percent
    def __init__(self, domain, percent):
        self.domain = domain
        self.percent = percent

class CountDomain:
    # function: find the top 50 percentage growth
    def __init__(self, days, tops):
    # parameter: days, tops, all integers
    # function: set up parameters, connect to database
        self.dayscount = days
        self.topcount = tops
        self.db = DatabaseConnector('localhost', 'root', 'ren39338', 'mailinglist')

    def CalDays(self):
    # return: a list, all items which the date is within dayscount
    # functions: return all domains that have mails in recent 'dayscount' days
        todaydate = datetime.date.today()
        limitdate = todaydate - datetime.timedelta(self.dayscount)
        cur = self.db.con.cursor()
        try:
            limitdateformat = limitdate.strftime('%Y-%m-%d')
            cur.execute("SELECT addrdomain, SUM(count) FROM domain WHERE date > %s GROUP BY addrdomain", (limitdateformat))
            matchresult = cur.fetchall()
            return matchresult
        except:
            return None

    def CalTotal(self, domain):
    # parameters: domain, a string
    # return: an integer, total number of mails from 'domain'
    # function: calculate total number of mails from 'domain'
        cur = self.db.con.cursor()
        try:
            cur.execute("SELECT SUM(count) FROM domain WHERE addrdomain = %s", (domain))
            countresult = cur.fetchone()
            if countresult:
                return countresult[0]
            else:
                return 0
        except:
            return 0

    def SortCount(self):
    # return: a list, sorted by percent
    # function: sort MatchDomain objects by percentage of growth
        sortedlist = sorted(self.matchlist, key=lambda x: x.percent, reverse=True)
        return sortedlist

    def GetTop(self):
    # return: list of top 50 domains
    # function: core function which call other functions to accomplish the task
        if self.topcount <= 0:
            return None
        recentdaysmatch = self.CalDays()
        if recentdaysmatch == None:
            return None
        self.matchlist = []
        for match in recentdaysmatch:
            matchdomain = match[0]
            matchcount = match[1]
            matchtotal = self.CalTotal(matchdomain)
            if matchtotal == 0:
                percent = 0.0
            else:
                percent = float(matchcount)/float(matchtotal)
            newitem = MatchDomain(matchdomain,percent)
            self.matchlist.append(newitem)
        sortedlist = self.SortCount()

        if len(sortedlist) <= 50:
            toplist = sortedlist
        else:
            toplist = sortedlist[0:49]
        return toplist




# script to run
m = MailingManagement()
m.insertMail('abc@motmail.com',datetime.date(2015,1,12))

p =  CountDomain(30,50)
l = p.GetTop()
print('domain   percent')
for e in l:
    print e.domain, e.percent


