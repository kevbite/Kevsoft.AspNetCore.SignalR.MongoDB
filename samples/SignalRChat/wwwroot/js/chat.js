"use strict";

var connection = new signalR.HubConnectionBuilder().withUrl("/chatHub").build();
var joinedGroups = new Set();

document.getElementById("sendButton").disabled = true;
document.getElementById("sendGroupButton").disabled = true;

connection.on("ReceiveMessage", function (user, message, group) {
    var li = document.createElement("li");
    document.getElementById("messagesList").appendChild(li);
    li.textContent = group
        ? `[${group}] ${user} says ${message}`
        : `${user} says ${message}`;
});

connection.on("JoinedGroup", function (group) {
    joinedGroups.add(group);
    updateGroupsDisplay();
});

connection.on("LeftGroup", function (group) {
    joinedGroups.delete(group);
    updateGroupsDisplay();
});

function updateGroupsDisplay() {
    var span = document.getElementById("joinedGroups");
    span.textContent = joinedGroups.size > 0 ? Array.from(joinedGroups).join(", ") : "(none)";
}

connection.start().then(function () {
    document.getElementById("sendButton").disabled = false;
    document.getElementById("sendGroupButton").disabled = false;
}).catch(function (err) {
    return console.error(err.toString());
});

document.getElementById("sendButton").addEventListener("click", function (event) {
    var user = document.getElementById("userInput").value;
    var message = document.getElementById("messageInput").value;
    connection.invoke("SendMessage", user, message).catch(function (err) {
        return console.error(err.toString());
    });
    event.preventDefault();
});

document.getElementById("joinGroupButton").addEventListener("click", function (event) {
    var group = document.getElementById("groupNameInput").value;
    if (!group) return;
    connection.invoke("JoinGroup", group).catch(function (err) {
        return console.error(err.toString());
    });
    event.preventDefault();
});

document.getElementById("leaveGroupButton").addEventListener("click", function (event) {
    var group = document.getElementById("groupNameInput").value;
    if (!group) return;
    connection.invoke("LeaveGroup", group).catch(function (err) {
        return console.error(err.toString());
    });
    event.preventDefault();
});

document.getElementById("sendGroupButton").addEventListener("click", function (event) {
    var user = document.getElementById("userInput").value;
    var group = document.getElementById("groupNameInput").value;
    var message = document.getElementById("groupMessageInput").value;
    if (!group) {
        alert("Enter a group name to send to.");
        return;
    }
    connection.invoke("SendMessageToGroup", user, group, message).catch(function (err) {
        return console.error(err.toString());
    });
    event.preventDefault();
});
