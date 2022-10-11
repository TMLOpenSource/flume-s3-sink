package com.tml.sinks;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;


public class CompressorFactoryTests {
    @Test
    public void shouldReturnGizCompressorForgzInput() {
        FileCompressor compressor = CompressorFactory.getInstance("gz");
        assertThat(compressor instanceof GizCompressor, is(true));
    }

    @Test
    public void shouldReturnNullCompressorForRandomInput() {
        FileCompressor compressor = CompressorFactory.getInstance("hello");
        assertThat(compressor instanceof NullCompressor, is(true));
    }
}
