#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const { randomUUID } = require("crypto");

const DEFAULT_USER_ID = "00000000-0000-0000-0000-000000000000";

function main() {
  const [, , ...args] = process.argv;
  if (args.length < 2) {
    console.error(
      "Usage: node convert-chatgpt-to-qwen.js <input-file> <output-file> [--limit=<n>]"
    );
    process.exit(1);
  }

  let limit = null;
  const positional = [];

  for (const arg of args) {
    if (typeof arg === "string" && arg.startsWith("--limit=")) {
      const value = arg.slice("--limit=".length);
      const parsed = Number.parseInt(value, 10);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        console.error("--limit must be a positive integer.");
        process.exit(1);
      }
      limit = parsed;
    } else if (!Number.isNaN(Number.parseInt(arg, 10)) && arg.trim() === String(Number.parseInt(arg, 10))) {
      const parsed = Number.parseInt(arg, 10);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        console.error("Limit value must be a positive integer.");
        process.exit(1);
      }
      limit = parsed;
    } else {
      positional.push(arg);
    }
  }

  if (positional.length < 2) {
    console.error(
      "Usage: node convert-chatgpt-to-qwen.js <input-file> <output-file> [--limit=<n>]"
    );
    process.exit(1);
  }

  const [inputPath, outputPath] = positional;
  const resolvedInput = path.resolve(process.cwd(), inputPath);
  const resolvedOutput = path.resolve(process.cwd(), outputPath);

  let rawInput;
  try {
    rawInput = fs.readFileSync(resolvedInput, "utf8");
  } catch (error) {
    console.error(`Failed to read input file at ${resolvedInput}: ${error.message}`);
    process.exit(1);
  }

  let conversations;
  try {
    conversations = JSON.parse(rawInput);
  } catch (error) {
    console.error(`Input file is not valid JSON: ${error.message}`);
    process.exit(1);
  }

  if (!Array.isArray(conversations)) {
    console.error("Expected the input JSON to be an array of ChatGPT conversations.");
    process.exit(1);
  }

  const sorted = sortConversations(conversations);
  const limited = typeof limit === "number" ? sorted.slice(0, limit) : sorted;

  const transformed = limited.map((conversation, index) =>
    transformConversation(conversation, index)
  );

  const output = {
    success: true,
    request_id: randomUUID(),
    data: transformed,
  };

  try {
    fs.writeFileSync(resolvedOutput, JSON.stringify(output, null, 2), "utf8");
  } catch (error) {
    console.error(`Failed to write output file at ${resolvedOutput}: ${error.message}`);
    process.exit(1);
  }

  const limitInfo = limit ? ` (latest ${limit})` : "";
  console.log(`Converted ${transformed.length} conversation(s)${limitInfo} to Qwen format.`);
}

function sortConversations(conversations) {
  return [...conversations].sort((a, b) => {
    const aTime = getComparableTimestamp(a);
    const bTime = getComparableTimestamp(b);
    return bTime - aTime;
  });
}

function getComparableTimestamp(conversation) {
  const updateTime = sanitizeTimestamp(conversation && conversation.update_time);
  const createTime = sanitizeTimestamp(conversation && conversation.create_time);
  const fallback = typeof createTime === "number" ? createTime : 0;
  return typeof updateTime === "number" ? updateTime : fallback;
}

function transformConversation(conversation, index) {
  const mapping = conversation && typeof conversation.mapping === "object" ? conversation.mapping : {};
  const orderedNodeIds = reconstructMessagePath(conversation);
  const includedIds = new Set();
  const messageMap = new Map();
  const messages = [];

  for (const nodeId of orderedNodeIds) {
    const node = mapping[nodeId];
    if (!node || !node.message) continue;

    const role = normalizeRole(node.message.author && node.message.author.role);
    if (role !== "user" && role !== "assistant") {
      continue;
    }

    const text = extractContent(node.message);
    if (!text) {
      continue;
    }

    const timestamp = sanitizeTimestamp(node.message.create_time);
    const qwenMessage = buildQwenMessage({
      node,
      role,
      text,
      timestamp,
      conversation,
    });

    includedIds.add(nodeId);
    messages.push(qwenMessage);
    messageMap.set(nodeId, qwenMessage);
  }

  // Wire up parent/children relationships after inclusion filtering
  for (const nodeId of includedIds) {
    const node = mapping[nodeId];
    const current = messageMap.get(nodeId);
    if (!current) continue;

    current.parentId = findIncludedAncestor(node.parent, includedIds, mapping);
    current.childrenIds = []; // reset to ensure clean array
  }

  for (const nodeId of includedIds) {
    const node = mapping[nodeId];
    const current = messageMap.get(nodeId);
    if (!current) continue;

    const parentId = current.parentId;
    if (parentId && messageMap.has(parentId)) {
      const parent = messageMap.get(parentId);
      parent.childrenIds.push(nodeId);
    }
  }

  const createdAt = sanitizeTimestamp(conversation.create_time);
  const updatedAt = sanitizeTimestamp(
    conversation.update_time !== undefined ? conversation.update_time : conversation.create_time
  );

  const lastAssistant = [...messages].reverse().find((msg) => msg.role === "assistant");
  const lastMessage = messages[messages.length - 1];
  const currentId = (lastAssistant || lastMessage)?.id || null;
  const currentResponseIds = currentId && lastAssistant ? [currentId] : [];

  const chatModels = collectModels(messages);

  const historyMessages = {};
  for (const message of messages) {
    historyMessages[message.id] = message;
  }

  return {
    id: conversation.conversation_id || conversation.id || `conversation-${index}`,
    user_id: DEFAULT_USER_ID,
    title: conversation.title || "Untitled Conversation",
    chat: {
      history: {
        messages: historyMessages,
        currentId,
        currentResponseIds,
      },
      models: chatModels.length > 0 ? chatModels : null,
      messages,
    },
    updated_at: updatedAt,
    created_at: createdAt,
    share_id: null,
    archived: false,
    pinned: false,
    meta: buildMeta(createdAt),
    folder_id: null,
    currentResponseIds,
    currentId,
    chat_type: null,
    models: chatModels.length > 0 ? chatModels : null,
  };
}

function collectModels(messages) {
  const models = new Set();
  for (const message of messages) {
    if (message.role === "assistant" && message.model) {
      models.add(message.model);
    }
    if (Array.isArray(message.models)) {
      for (const model of message.models) {
        if (model) {
          models.add(model);
        }
      }
    }
  }
  return Array.from(models);
}

function buildMeta(createdAt) {
  const timestamp = typeof createdAt === "number" && !Number.isNaN(createdAt)
    ? createdAt * 1000
    : null;

  if (timestamp) {
    return { timestamp, tags: [] };
  }
  return { tags: [] };
}

function buildQwenMessage({ node, role, text, timestamp, conversation }) {
  const id = node.message.id || node.id;
  const defaultModel = conversation.default_model_slug || null;
  const modelSlug = (node.message.metadata && node.message.metadata.model_slug) || defaultModel;

  if (role === "assistant") {
    return {
      role: "assistant",
      content: "",
      reasoning_content: null,
      chat_type: null,
      sub_chat_type: null,
      model: modelSlug || null,
      modelName: formatModelName(modelSlug),
      modelIdx: 0,
      id,
      parentId: null,
      childrenIds: [],
      feature_config: null,
      content_list: [
        {
          content: text,
          phase: "answer",
          status: "finished",
          extra: null,
          role: "assistant",
          usage: null,
        },
      ],
      is_stop: false,
      edited: false,
      error: null,
      meta: {},
      extra: null,
      feedbackId: null,
      turn_id: null,
      annotation: null,
      done: true,
      info: null,
      timestamp,
    };
  }

  return {
    id,
    role: "user",
    content: text,
    models: modelSlug ? [modelSlug] : [],
    chat_type: null,
    sub_chat_type: null,
    edited: false,
    error: null,
    extra: null,
    feature_config: null,
    parentId: null,
    turn_id: null,
    childrenIds: [],
    files: [],
    timestamp,
  };
}

function formatModelName(slug) {
  if (!slug) return null;
  return slug
    .split(/[-_]/)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function findIncludedAncestor(nodeId, includedIds, mapping) {
  let currentId = nodeId;
  while (currentId) {
    if (includedIds.has(currentId)) {
      return currentId;
    }
    const parent = mapping[currentId] && mapping[currentId].parent;
    currentId = parent || null;
  }
  return null;
}

function reconstructMessagePath(conversation) {
  const mapping = conversation && typeof conversation.mapping === "object" ? conversation.mapping : {};
  const nodeIds = Object.keys(mapping);
  if (nodeIds.length === 0) {
    return [];
  }

  const rootNodes = findRootNodes(mapping);
  if (rootNodes.length === 0) {
    return [];
  }

  if (conversation.current_node && mapping[conversation.current_node]) {
    const path = getPathToRoot(conversation.current_node, mapping);
    return expandPath(path, mapping);
  }

  return traverseFromNode(rootNodes[0], mapping, new Set());
}

function expandPath(path, mapping) {
  const result = [];
  for (const nodeId of path) {
    result.push(nodeId);
    const node = mapping[nodeId];
    if (node && Array.isArray(node.children) && node.children.length > 0) {
      const childTraversal = traverseFromNode(node.children[0], mapping, new Set(result));
      result.push(...childTraversal);
      break;
    }
  }
  return Array.from(new Set(result));
}

function traverseFromNode(nodeId, mapping, visited) {
  if (!nodeId || visited.has(nodeId)) {
    return [];
  }

  visited.add(nodeId);
  const node = mapping[nodeId];
  if (!node) {
    return [];
  }

  const sequence = [nodeId];
  if (Array.isArray(node.children) && node.children.length > 0) {
    for (const childId of node.children) {
      const childPath = traverseFromNode(childId, mapping, visited);
      if (childPath.length > 0) {
        sequence.push(...childPath);
        break; // follow first branch for linear flow
      }
    }
  }

  return sequence;
}

function findRootNodes(mapping) {
  return Object.keys(mapping).filter((nodeId) => {
    const node = mapping[nodeId];
    if (!node) return false;
    if (!node.parent) return true;
    return !mapping[node.parent];
  });
}

function getPathToRoot(nodeId, mapping) {
  const path = [];
  let currentId = nodeId;
  while (currentId && mapping[currentId]) {
    path.unshift(currentId);
    currentId = mapping[currentId].parent || null;
  }
  return path;
}

function normalizeRole(role) {
  switch (role) {
    case "user":
      return "user";
    case "assistant":
      return "assistant";
    default:
      return role || "assistant";
  }
}

function extractContent(message) {
  if (!message || !message.content) return "";
  const parts = message.content.parts;
  if (!Array.isArray(parts)) return "";

  const textParts = [];
  for (const part of parts) {
    if (typeof part === "string") {
      textParts.push(part);
    } else if (part && typeof part === "object") {
      if (typeof part.text === "string") {
        textParts.push(part.text);
      } else if (part.asset_pointer) {
        textParts.push("[Asset]");
      }
    }
  }

  return textParts.join("\n").trim();
}

function sanitizeTimestamp(value) {
  if (typeof value === "number" && !Number.isNaN(value)) {
    return Math.floor(value);
  }
  if (typeof value === "string") {
    const parsed = parseFloat(value);
    if (!Number.isNaN(parsed)) {
      return Math.floor(parsed);
    }
  }
  return null;
}

main();
