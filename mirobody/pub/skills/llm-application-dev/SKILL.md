---
name: llm-application-dev
description: Building applications with Large Language Models - prompt engineering, RAG patterns, and LLM integration. Use for AI-powered features, chatbots, or LLM-based automation.
source: wshobson/agents
license: MIT
---

# LLM Application Development

## Prompt Engineering

### Structured Prompts
```typescript
const systemPrompt = `You are a helpful assistant that answers questions about our product.

RULES:
- Only answer questions about our product
- If you don't know, say "I don't know"
- Keep responses concise (under 100 words)
- Never make up information

CONTEXT:
{context}`;

const userPrompt = `Question: {question}`;
```

### Few-Shot Examples
```typescript
const prompt = `Classify the sentiment of customer feedback.

Examples:
Input: "Love this product!"
Output: positive

Input: "Worst purchase ever"
Output: negative

Input: "It works fine"
Output: neutral

Input: "${customerFeedback}"
Output:`;
```

### Chain of Thought
```typescript
const prompt = `Solve this step by step:

Question: ${question}

Let's think through this:
1. First, identify the key information
2. Then, determine the approach
3. Finally, calculate the answer

Step-by-step solution:`;
```

## API Integration

### OpenAI Pattern
```typescript
import OpenAI from 'openai';

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

async function chat(messages: Message[]): Promise<string> {
  const response = await openai.chat.completions.create({
    model: 'gpt-4',
    messages,
    temperature: 0.7,
    max_tokens: 500,
  });

  return response.choices[0].message.content ?? '';
}
```

### Anthropic Pattern
```typescript
import Anthropic from '@anthropic-ai/sdk';

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

async function chat(prompt: string): Promise<string> {
  const response = await anthropic.messages.create({
    model: 'claude-3-opus-20240229',
    max_tokens: 1024,
    messages: [{ role: 'user', content: prompt }],
  });

  return response.content[0].type === 'text'
    ? response.content[0].text
    : '';
}
```

### Streaming Responses
```typescript
async function* streamChat(prompt: string) {
  const stream = await openai.chat.completions.create({
    model: 'gpt-4',
    messages: [{ role: 'user', content: prompt }],
    stream: true,
  });

  for await (const chunk of stream) {
    const content = chunk.choices[0]?.delta?.content;
    if (content) yield content;
  }
}
```

## RAG (Retrieval-Augmented Generation)

### Basic RAG Pipeline
```typescript
async function ragQuery(question: string): Promise<string> {
  // 1. Embed the question
  const questionEmbedding = await embedText(question);

  // 2. Search vector database
  const relevantDocs = await vectorDb.search(questionEmbedding, { limit: 5 });

  // 3. Build context
  const context = relevantDocs.map(d => d.content).join('\n\n');

  // 4. Generate answer
  const prompt = `Answer based on this context:\n${context}\n\nQuestion: ${question}`;
  return await chat(prompt);
}
```

### Document Chunking
```typescript
function chunkDocument(text: string, options: ChunkOptions): string[] {
  const { chunkSize = 1000, overlap = 200 } = options;
  const chunks: string[] = [];

  let start = 0;
  while (start < text.length) {
    const end = Math.min(start + chunkSize, text.length);
    chunks.push(text.slice(start, end));
    start += chunkSize - overlap;
  }

  return chunks;
}
```

### Embedding Storage
```typescript
// Using Supabase with pgvector
async function storeEmbeddings(docs: Document[]) {
  for (const doc of docs) {
    const embedding = await embedText(doc.content);

    await supabase.from('documents').insert({
      content: doc.content,
      metadata: doc.metadata,
      embedding: embedding,  // vector column
    });
  }
}

async function searchSimilar(query: string, limit = 5) {
  const embedding = await embedText(query);

  const { data } = await supabase.rpc('match_documents', {
    query_embedding: embedding,
    match_count: limit,
  });

  return data;
}
```

## Error Handling

```typescript
async function safeLLMCall<T>(
  fn: () => Promise<T>,
  options: { retries?: number; fallback?: T }
): Promise<T> {
  const { retries = 3, fallback } = options;

  for (let i = 0; i < retries; i++) {
    try {
      return await fn();
    } catch (error) {
      if (error.status === 429) {
        // Rate limit - exponential backoff
        await sleep(Math.pow(2, i) * 1000);
        continue;
      }
      if (i === retries - 1) {
        if (fallback !== undefined) return fallback;
        throw error;
      }
    }
  }
  throw new Error('Max retries exceeded');
}
```

## Best Practices

- **Token Management**: Track usage and set limits
- **Caching**: Cache embeddings and common queries
- **Evaluation**: Test prompts with diverse inputs
- **Guardrails**: Validate outputs before using
- **Logging**: Log prompts and responses for debugging
- **Cost Control**: Use cheaper models for simple tasks
- **Latency**: Stream responses for better UX
- **Privacy**: Don't send PII to external APIs
