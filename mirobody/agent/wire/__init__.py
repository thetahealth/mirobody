"""LangGraph's stream → what a client renders.

    events_bridge.py  one reading of the stream into `kernel.events`, the
                      wire-neutral vocabulary: usage, reasoning, text, tool
                      call started/arguments/completed, tool result with its
                      artifact status, interrupt with every pending action
    stream.py         `StreamConverter`, this repository's chunk dialect
                      rendered over those events

Two layers because the second is a product decision and the first is not: a
consumer whose client speaks its own frames binds `events_bridge` and writes
its own renderer, and both then agree about what happened in the turn.
"""
