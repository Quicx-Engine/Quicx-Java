/**
 * Java client for Quicx, a task queue daemon with a hard memory limit.
 *
 * <p>{@link dev.quicx.QuicxClient} submits tasks and can wait for their
 * result; {@link dev.quicx.QuicxWorker} takes tasks and runs them. Both talk
 * to quicxd over QXP, the Quicx Protocol, and work with Quicx 1.0.3 and later;
 * value levels and evictions need quicxd 2.1.0. The library has no
 * dependencies and needs Java 11.
 */
package dev.quicx;
