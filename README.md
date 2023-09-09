# 📢Event Notification System

⚠ *The current version is a prototype and is not intended for real use. The latest release is paid and protected by consumer rights* ⚠ <br/>

---
▷ **The main goal of the project:** <br/>
receive and process information about various events (concerts, movies, performances, etc.)

---
### ☰ The project consists of two parts:
- Ticketmaster API parser [[tm]](https://github.com/qqlaker/Ticket-Notification/tree/main/tm )
- Twitter (X) API parser [[tw]](https://github.com/qqlaker/Ticket-Notification/tree/main/tw )

Both parts are managed from a file [server.py](https://github.com/qqlaker/Ticket-Notification/blob/main/server.py) <br/>
This approach ensures fault tolerance of the system <br/>

Both scenarios receive filtered data from the official API and send it to the Discord server, previously divided into categories

---

#### ▽ Prewiew:
*Events from ticketmaster* <br/>
![events from ticketmaster](https://i.postimg.cc/sgtxqymH/2023-09-09-1341651.png) <br/><br/>
*Events from twitter*<br/>
![events from twitter](https://i.postimg.cc/1zqn344Z/2023-09-09-2.png)

---

### ✅ This way we can track upcoming events in advance ✅
