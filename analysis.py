#!/usr/bin/python
# -*- coding: cp936 -*-
import MySQLdb
import MySQLdb.cursors
import fileinput
import os
import sys
import csv
from collections import defaultdict
from collections import Counter
from datetime import date, timedelta

class AnalysisReportMailingsTrend(object):
    """The class analyizes a csv files and connects Mysql database"""

    lst = defaultdict(list)
    dict_lst = defaultdict(list)
    domainPR_lst = defaultdict(list)

    def __init__(self):
        super(AnalysisReportMailingsTrend, self).__init__()
      
        
    def get_file_path(self,filename):
        self.currentdirpath = os.getcwd()
        self.filepath = os.path.join(self.currentdirpath, filename)
        return self.filepath


    
    def read_CSV(self,filepath):
    
       self.domain_list = []
       self.domain_date_list = []
       self.sorted_domain_list_bydate = defaultdict(list)
          
       with open(filepath, 'rb') as csvfile:
           self.reader = csv.reader(csvfile)
           
           for self.row in self.reader:
               # input the email and date to a input list
               self.email = self.row[0].strip().lower()
               self.date  = self.row[1].strip()
     
               
               self.domain_date_list.append([self.date, self.email[ self.email.find("@") : ]])
               self.domain_list.append(self.email[ self.email.find("@") : ])
               
       for key, value in self.domain_date_list: 
             self.sorted_domain_list_bydate[key].append(value)
                   
               
       # remove duplicates from domain list
       self.domain_list = list(set(self.domain_list))
       
       return self.sorted_domain_list_bydate, self.domain_list

    def update_DB(self,lst):
    
        # connect mysql database
       try:
           self.conn = MySQLdb.connect("localhost","root","j3xu","TESTDB" )
       except:
           print ("connection to the database failed")
        
       # use the dictionary module
       self.cur = conn.cursor()

       self.cur.execute("DROP TABLE IF EXISTS mailing CASCADE")

       self.sqlcmd = """ CREATE TABLE mailing (
                 addr varchar(255) NOT NULL) """

       self.cur.execute(self.sqlcmd)

       self.cur.execute("DROP TABLE IF EXISTS domains CASCADE")
        
       self.sqlcmd = """CREATE TABLE domains(
                id bigint not null auto_increment primary key,
                domain_name varchar(20) NOT NULL,
                cnt int,
                date_of_entry date NOT NULL)"""
        
       self.cur.execute(self.sqlcmd) 
        
        
       # counters the lst's values
        
       self.array = []
       for key, value in lst.items():
           self.array = value # store the values of the ith element of lst in array
           self.hash_list = Counter(array) # add Counter and store in hashable list
           for key1, value1 in self.hash_list.iteritems():
              self.query = "INSERT INTO domains(domain_name, cnt, date_of_entry) VALUES (%s, %s, %s);"
              self.data  = (key1, value1, key)
              self.cur.execute(query, data)
               
       self.conn.commit()
       self.conn.close()

    def calc_PR(self,lst, dict_lst,targetdays):
    

        domainPR_lst = {}
    
    
        self.days_to_subtract = targetdays  # Calc a certain days from current date (call it target_date)
        self.target_date = date.today() - timedelta(days=self.days_to_subtract)
        
          # connect database
        try:

            self.conn = MySQLdb.connect("localhost","root","j3xu","TESTDB" )   
        except:
            print ("connection to the database failed")
            

        self.cur = self.conn.cursor()
        
        for self.dom_name in dict_lst:
            
            # initialization
            self.v_present=0.0
            self.v_pastduration=0.0
            self.v_past=0.0
            self.PR_targetdate = 0.0     
            self.PR_total = 0.0          
            
           # query1: counts the number of users for each domain within a peroid of time  (call it v_pastduration).
            self.query1 = "SELECT coalesce(cnt,0) FROM domains WHERE date_of_entry = %s AND domain_name = %s"
            self.data1 = (self.target_date, self.dom_name)
            self.cur.execute(self.query1, self.data1)
            self.rows_query1=self.cur.fetchall()
            for self.row in self.rows_query1: 
                self.v_pastduration = self.row[0]  
            
                   
            # query2: counts the number of users for each domain that occured today (call it v_present).
            self.query2 = "SELECT COALESCE(cnt, 0) FROM domains WHERE date_of_entry = %s AND domain_name = %s;"
            self.data2 = (date.today(), self.dom_name)
            self.cur.execute(self.query2, self.data2)
            self.rows_query2 = self.cur.fetchall()  
            for self.row in self.rows_query2:
                self.v_present = self.row[0]  
        
            
            # calculate perecntage of growth rate from today to certain date before (called PR_targetdate) 
            if self.v_pastduration == 0: 
                self.PR_targetdate = -0xDEF # stands for undefined
            else : 
                self.PR_targetdate = ((self.v_present - self.v_pastduration)/ float(self.v_pastduration)) * 100.0 
        

            # query3: counts the number of users for each domain that occured in the earliest date (call it v_past).
            self.query3 = "SELECT COALESCE(cnt,0) FROM domains WHERE domain_name = %s AND date_of_entry=(select min(date_of_entry) FROM domains WHERE domain_name=%s);"
            self.data3 = (self.dom_name, self.dom_name)
            self.cur.execute(self.query3, self.data3)
            self.rows_query3 = self.cur.fetchall()  
            for self.row in self.rows_query3:
                self.v_past = self.row[0]

         
            
            # Calculate perecntage of total growth rate for (called PR_total) 
            if self.v_past == 0: 
                self.PR_total = -0xDEF 
            else:
                self.PR_total = ((self.v_present - self.v_past)/ float(self.v_past) ) * 100.00 
                
                 
            # fill the list: domainPR_lst
            if (self.PR_targetdate == -0xDEF) or (self.PR_total == -0xDEF): 
                 domainPR_lst[self.dom_name] = -0xDEF
            elif (self.PR_total==0):
                 domainPR_lst[self.dom_name] = -0xDEF
            else:
                 domainPR_lst[self.dom_name] = self.PR_targetdate/self.PR_total
        
                 
        self.conn.close()
        
     
        domainPR_lst = [(key, domainPR_lst[key]) for key in sorted(domainPR_lst, key=domainPR_lst.get, reverse=True)]
        
        return domainPR_lst

    def printReport(self,domainPR_lst,domainsnumber):
    
  
      max = domainsnumber
      self.count = 1
      try:
        with open('Result_Report.csv', 'w') as files:
            self.writer = csv.writer(files)    
            
            for self.value in domainPR_lst:
                 self.writer.writerow(self.value)
                 self.count +=1
                 if (self.count > max): break
           
            
            print ("===========================================================")
            print ("")
            print ("╭ ￣ ￣ ╮　　　　　 　　　╭ ￣ ￣ ╮         ")
            print ("/ 　╭￣ ╮ ￣￣￣￣￣￣￣/　╭ ￣╮　         ")
            print ("　╰╯ ｜　　　　　　 　　 ｜ ╰╯ 　 /.        ")
            print ("╰ ＿＿ ╯　／￣＼ 　／￣＼　╰ ＿＿ ╯        ")
            print ("　　　 ｜ 　　　　　　　　　｜              ")
            print ("　　　　＼　　　╰^╯　　　 ／               ")
            print ("　　　　　╰ ?＿＿＿＿＿ ╯                 ")
            print ("")
            print ("===========================================================")
            print ("#Report is in the file: /Result_Report.csv")
       
      except IOError as e:
           print "#>>>>>Result_Report.csv open file failed" 

#==================================================================================================

if __name__ == '__main__':


    print ("#=====================Read Me ===========================================================")
    print ("#A Task to analysis csv file log to generate a report ")
    print ("#The input csv file name: <-- the input csv file name (email.csv)")
    print ("#The days(digit) before today: <-- the period before today (30)")
    print ("#The maximum items(digit) in Result Report: <-- the number of items in Result Report(50) ")
    print ("#=====================Read Me ===========================================================")
    
    inputfile = raw_input("#>>>>>The input csv file name:")
    inputdays = input("#>>>>>The days(number) before today:")
    inputmaxitem = input("#>>>>>The maximum items(number) in Result Report:")
    
    AnalysisReportMail = AnalysisReportMailingsTrend()

    
    path=AnalysisReportMail.get_file_path(inputfile)
      
    [lst, dict_lst] = AnalysisReportMail.read_CSV(path) # read the input file

    domainPR_lst={}
    domainPR_lst = AnalysisReportMail.calc_PR(lst, dict_lst,inputdays) 
    AnalysisReportMail.printReport(domainPR_lst,inputmaxitem)




