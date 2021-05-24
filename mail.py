import smtplib
class EmailClass:
    def __init__(self):
        pass
    def email(self,listoffiles,sender,password):
        string = ""
        count = 1
        for i in listoffiles:
            string = string + str(count) + ".  " + str(i) + "\n"
            count = count + 1

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, "suryajith082@gmail.com","Bad files after validation are\n\n\n"+string)

