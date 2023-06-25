package bachelor.core.utils.generate

import bachelor.core.api.snapshot.Phase.*
import com.google.common.truth.Truth.assertThat
import org.junit.jupiter.api.Test


class TimelineParserTest {
    @Test
    fun podEvents() {
        val podEvents =
            "|A(P/U)-|U(P/U)-|U(P/W)-|U(R/R)-|-------|U(R/T0)-|-------|U(S/T-2)|-------|-------|U(S/T0)|D(S/T0)|"
        val expected = listOf(
            add(PENDING),
            upd(PENDING),
            upd(PENDING, waiting()),
            upd(RUNNING, running()),
            noop(),
            upd(RUNNING, terminated(0)),
            noop(),
            upd(SUCCEEDED, terminated(-2)),
            noop(),
            noop(),
            upd(SUCCEEDED, terminated(0)),
            del(SUCCEEDED, terminated(0))
        )

        val result = parsePodEvents(podEvents)
        assertThat(result)
            .containsExactlyElementsIn(expected)
    }

    @Test
    fun jobEvents() {
        val jobEvents =
            "|A(nnnn)|U(10nn)|-------|-------|U(11nn)|--------|U(10nn)|-------|U(n0n1)|D(n0n1)|-------|-------|"
        val expected = listOf(
            add(null, null, null, null),
            upd(1, 0, null, null),
            noop(),
            noop(),
            upd(1, 1, null, null),
            noop(),
            upd(1, 0, null, null),
            noop(),
            upd(null, 0, null, 1),
            del(null, 0, null, 1),
            noop(),
            noop()
        )

        val result = parseJobEvents(jobEvents)
        assertThat(result)
            .containsExactlyElementsIn(expected)
    }
}