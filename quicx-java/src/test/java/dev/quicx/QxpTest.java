package dev.quicx;

import dev.quicx.internal.Frame;
import dev.quicx.internal.Qxp;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class QxpTest {

    @Test
    void frameRoundTrip() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        Qxp.writeFrame(bytes, Qxp.DONE, Qxp.doneBody(0x01020304));
        byte[] wire = bytes.toByteArray();
        assertArrayEquals(new byte[] {1, 0x05, 0, 0, 0, 4, 1, 2, 3, 4}, wire);

        Frame f = Qxp.readFrame(new ByteArrayInputStream(wire));
        assertEquals(Qxp.DONE, f.type);
        assertEquals(0x01020304, Qxp.getInt(f.body, 0));
    }

    @Test
    void submitBodyLayout() {
        byte[] body = Qxp.submitBody("mail", "hi".getBytes(StandardCharsets.UTF_8));
        assertArrayEquals(new byte[] {4, 'm', 'a', 'i', 'l', 'h', 'i'}, body);
    }

    @Test
    void largestPayloadFitsAndOneMoreByteDoesNot() {
        String type = "mail";
        int max = Qxp.MAX_BODY - 1 - type.length();
        assertEquals(Qxp.MAX_BODY, Qxp.submitBody(type, new byte[max]).length);
        assertThrows(IllegalArgumentException.class, () -> Qxp.submitBody(type, new byte[max + 1]));
    }

    @Test
    void valuedSubmitBodyPutsTheLevelFirst() {
        byte[] body = Qxp.valuedSubmitBody(12, "mail", "hi".getBytes(StandardCharsets.UTF_8), Qxp.MAX_BODY);
        assertArrayEquals(new byte[] {12, 4, 'm', 'a', 'i', 'l', 'h', 'i'}, body);
        assertEquals(0x0D, Qxp.SUBMIT_VALUED);
    }

    @Test
    void levelsAreZeroToFifteen() {
        assertEquals(0, Qxp.valuedSubmitBody(0, "a", null, Qxp.MAX_BODY)[0]);
        assertEquals(15, Qxp.valuedSubmitBody(15, "a", null, Qxp.MAX_BODY)[0]);
        assertThrows(IllegalArgumentException.class, () -> Qxp.valuedSubmitBody(-1, "a", null, Qxp.MAX_BODY));
        assertThrows(IllegalArgumentException.class, () -> Qxp.valuedSubmitBody(16, "a", null, Qxp.MAX_BODY));
    }

    @Test
    void theLevelByteCountsTowardTheLimit() {
        String type = "mail";
        int max = Qxp.MAX_BODY - 2 - type.length();
        assertEquals(Qxp.MAX_BODY, Qxp.valuedSubmitBody(1, type, new byte[max], Qxp.MAX_BODY).length);
        assertThrows(IllegalArgumentException.class, () -> Qxp.valuedSubmitBody(1, type, new byte[max + 1], Qxp.MAX_BODY));
    }

    @Test
    void aBiggerDaemonTakesBiggerBodies() {
        assertThrows(IllegalArgumentException.class, () -> Qxp.submitBody("img", new byte[8000]));
        assertEquals(8004, Qxp.submitBody("img", new byte[8000], 8192).length);
        assertEquals(8005, Qxp.valuedSubmitBody(3, "img", new byte[8000], 8192).length);
    }

    @Test
    void taskTypeLimits() {
        assertEquals(31, Qxp.typeBytes("a".repeat(31)).length);
        assertThrows(IllegalArgumentException.class, () -> Qxp.typeBytes("a".repeat(32)));
        assertThrows(IllegalArgumentException.class, () -> Qxp.typeBytes(""));
        assertThrows(IllegalArgumentException.class, () -> Qxp.typeBytes(null));
        assertThrows(IllegalArgumentException.class, () -> Qxp.typeBytes("a\0b"));
        // 11 three-byte characters are 33 bytes, too long although only 11 characters
        assertThrows(IllegalArgumentException.class, () -> Qxp.typeBytes("€".repeat(11)));
    }

    @Test
    void failedReasonIsCutAtACharacterBoundary() {
        String reason = "x".repeat(62) + "€€";   // 62 + 6 bytes
        byte[] body = Qxp.failedBody(9, reason);
        String back = new String(body, 4, body.length - 4, StandardCharsets.UTF_8);
        assertEquals("x".repeat(62), back);   // "€" would need bytes 63-65, so it is dropped whole
        assertTrue(body.length - 4 <= Qxp.MAX_REASON_BYTES);
    }

    @Test
    void wrongVersionAndClosedStreamAreErrors() {
        assertThrows(IOException.class, () -> Qxp.readFrame(new ByteArrayInputStream(new byte[] {2, 1, 0, 0, 0, 0})));
        assertThrows(EOFException.class, () -> Qxp.readFrame(new ByteArrayInputStream(new byte[] {1, 2, 0})));
    }

    @Test
    void errorCodes() {
        assertEquals(ErrorCode.QUEUE_FULL, ErrorCode.of(1));
        assertEquals(ErrorCode.VERSION_MISMATCH, ErrorCode.of(5));
        assertEquals(ErrorCode.UNKNOWN, ErrorCode.of(77));
    }
}
