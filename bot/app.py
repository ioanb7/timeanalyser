#python3 -m pip install -U https://github.com/Rapptz/discord.py/archive/rewrite.zip
import discord
import asyncio
import os
import datetime
import sys

class MyClient(discord.Client):
    async def on_ready(self):
        print('Authenticating in as')
        print(self.user.name)
        print(self.user.id)
        print('------')
        
        self.db = []

        await self.change_presence(activity=discord.Game(name="Downloading your data"))
        for server in self.guilds:
            print("I am on server:" + str(server))
            
        await self.load_db()
    async def load_db(self):
        db = []
        with open('database.txt', 'r', encoding='utf-8') as the_file:
            for line in the_file:
                line = line.strip()
                line_s = line.split("\t")
                print(line_s)
                db += [(line_s[0], int(line_s[1]), )]
               
        self.db = db
        
    async def backup(self):
        with open('rawdata.txt', 'a+', encoding='utf-8') as the_file:
            for server in self.guilds:
                for channel in server.text_channels:
                    print("Parsing :" + str(server) + ":" + str(channel))
                    try:
                        async for message in channel.history(limit=999999):
                            seconds = (message.created_at -datetime.datetime(1970,1,1)).total_seconds()
                            seconds = int(seconds * 1000)
                            strstr = str(message.id) + "," + str(channel.id) + "," + str(seconds) + "," + str(message.author.id) + "," + message.author.name + "," + message.content
                            
                            #print(strstr)
                            sys.stdout.write('.')
                            sys.stdout.flush()
                            the_file.write(strstr + "\n")
                    except discord.errors.Forbidden:
                        print("No access")
                    print("")
        
        print("DONE saving messages")
    async def on_message(self, message):
        print('Message from {0.author}: {0.content}'.format(message))
        if message.author.id == 209662820965548032:
            if message.content == "!archive":
                await self.backup()
            if message.content == "!reload":
                await self.load_db()
        if message.content.startswith("!activity"):
            user_id = str(message.author.id)
            if len(message.mentions) > 0:
                user_id = str(message.mentions[0].id)
            
            found = False
            seconds = 0
            for line in self.db:
                if line[0] == user_id:
                    seconds = line[1]
                    found=True
                    break
            
            if found:
                minutes = round(seconds / 60, 2)
                minutes = str(minutes)
                await message.channel.send("Replying every " + minutes + " minutes. Not bad.")
            else:
                await message.channel.send("Couldn't find ya")
    
    
accesskey = input('Access key:')
print("OK")
client = MyClient()
client.run(accesskey)
print("BOT TERMINATED HERE.")