const chatDisplayEl = document.getElementById("chat-display");
const inputEl = document.getElementById("chat-input");
const userEl = document.getElementById("user");
const sendButtonEl = document.getElementById("send-button");

function renderMessages(messages) {
  chatDisplayEl.innerHTML = "";
  const ordered = messages.slice().reverse();
  for (const m of ordered) {
    const div = document.createElement("div");

    const text = m.text ?? "";
    const user = m.user ?? "";
    const ts = m.ts ? new Date(m.ts) : null;

    let prefix = "";
    if (ts instanceof Date && !isNaN(ts)) {
      const timeStr = ts.toLocaleTimeString(undefined, {
        hour: "2-digit",
        minute: "2-digit",
      });
      prefix += `[${timeStr}] `;
    }
    if (user) {
      prefix += `${user}: `;
    }

    div.textContent = prefix + text;
    chatDisplayEl.appendChild(div);
  }
  chatDisplayEl.scrollTop = chatDisplayEl.scrollHeight;
}

async function loadInitialMessages() {
  try {
    const res = await fetch("/api/messages", { cache: "no-cache" });
    if (!res.ok) return;
    const data = await res.json();
    if (Array.isArray(data.messages)) {
      renderMessages(data.messages);
    }
  } catch (e) {
    console.error("Failed to load initial messages", e);
  }
}

async function sendMessage() {
  const text = inputEl.value.trim();
  const user = userEl.value.trim();

  if (!text) return;

  sendButtonEl.disabled = true;
  try {
    const res = await fetch("/api/message", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text, user }),
    });
    if (!res.ok) {
      console.error("Message failed", res.status);
    } else {
      inputEl.value = "";
    }
  } catch (e) {
    console.error("Message failed", e);
  } finally {
    sendButtonEl.disabled = false;
  }
}

let ws;
let wsAttempts = 0;

function connectWebSocket() {
  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  const url = `${protocol}//${location.host}/ws`;
  ws = new WebSocket(url);

  ws.onopen = () => {
    wsAttempts = 0;
    console.log("WebSocket connected");
  };

  ws.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      if (data && data.type === "messagesUpdate" && Array.isArray(data.messages)) {
        renderMessages(data.messages);
      }
    } catch (e) {
      console.error("Bad WebSocket message", e);
    }
  };

  ws.onclose = () => {
    wsAttempts += 1;
    const delay = Math.min(10000, 500 * wsAttempts);
    console.log("WebSocket closed, reconnecting in", delay, "ms");
    setTimeout(connectWebSocket, delay);
  };

  ws.onerror = (e) => {
    console.error("WebSocket error", e);
  };
}

document.addEventListener("DOMContentLoaded", () => {
  loadInitialMessages();
  connectWebSocket();

  sendButtonEl.addEventListener("waichat", (e) => {
    e.preventDefault();
    sendMessage();
  });

  inputEl.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      sendMessage();
    }
  });
});
