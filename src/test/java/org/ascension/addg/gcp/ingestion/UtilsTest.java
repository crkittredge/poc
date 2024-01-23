package org.ascension.addg.gcp.ingestion;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretPayload;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.*;

/**
 * Tests the Utils class
 */
public class UtilsTest {

    /**
     * Set up the test
     */
    public void Utils() {
        // no nothing
    }

    /**
     * Tests the positive scenario of readFromGCS
     */
    @Test public void readFromGCS(){

        class MockReadableByteChannel implements ReadableByteChannel {
            private boolean open = true;
            private final ByteBuffer bb;

            public MockReadableByteChannel(String content) {
                this.bb = ByteBuffer.allocate(content.length()).put(content.getBytes(StandardCharsets.UTF_8)).rewind();
            }

            @Override
            public boolean isOpen() {
                return this.open;
            }

            @Override
            public void close() {
                this.open = false;
            }

            @Override
            public int read(ByteBuffer dst) {
                final ByteBuffer src = this.bb;
                int retVal = 0;

                if (src.remaining() == 0) {
                    retVal = -1;
                }

                int n = 0;
                while ((src.remaining() != 0) && (dst.remaining() != 0)) {
                    dst.put(src.get());
                    retVal = ++n;
                }

                return retVal;
            }
        };

        var resourceIdMock = mock(ResourceId.class);
        var rbcMock = new MockReadableByteChannel("some file content");

        try (var fsMockedStatic = mockStatic(FileSystems.class)) {
            fsMockedStatic.when(() -> FileSystems.matchNewResource(any(), anyBoolean())).thenReturn(resourceIdMock);
            fsMockedStatic.when(() -> FileSystems.open(resourceIdMock)).thenReturn(rbcMock);
            var result = Utils.readFromGCS(ValueProvider.StaticValueProvider.of("gs://some_bucket/some_file.txt"));
            assertEquals("some file content", result.get());
        }
    }

    /**
     * Tests the exception scenario of readFromGCS
     */
    @Test public void readFromGCSException(){
        var resourceIdMock = mock(ResourceId.class);

        try (var fsMockedStatic = mockStatic(FileSystems.class)) {
            fsMockedStatic.when(() -> FileSystems.matchNewResource(any(), anyBoolean())).thenReturn(resourceIdMock);
            fsMockedStatic.when(() -> FileSystems.open(resourceIdMock)).thenThrow(IOException.class);
            assertThrows(Utils.SchemaExtractionException.class, () -> Utils.readFromGCS(ValueProvider.StaticValueProvider.of("gs://some_bucket/some_other_file.txt")).get());
        }
    }

    /**
     * Tests the positive scenario of getSecret
     */
    @Test public void getSecret(){
        var smClientMock = mock(SecretManagerServiceClient.class);
        try (var smClientMockedStatic = mockStatic(SecretManagerServiceClient.class)) {
            smClientMockedStatic.when(SecretManagerServiceClient::create).thenReturn(smClientMock);
            var secretResponse = AccessSecretVersionResponse.newBuilder().setPayload(SecretPayload.newBuilder()
                    .setData(ByteString.copyFrom("secret data", StandardCharsets.UTF_8)).build()).build();
            doReturn(secretResponse).when(smClientMock).accessSecretVersion(any(SecretVersionName.class));
            var secret = Utils.CredentialsHelper.getSecret("some_project", "some_secret");
            assertEquals(secretResponse.getPayload().getData().toStringUtf8(), secret);
        }
    }

    /**
     * Tests the exception scenario of getSecret
     */
    @Test public void getSecretException(){
        try (var smClientMockedStatic = mockStatic(SecretManagerServiceClient.class)) {
            smClientMockedStatic.when(SecretManagerServiceClient::create).thenThrow(IOException.class);
            assertThrows(Utils.CredentialsHelper.GetCredentialsException.class, () -> Utils.CredentialsHelper.getSecret("some_project", "some_secret"));
        }
    }

    /**
     * Tests the positive scenario of checkHeaderName
     */
    @Test public void checkHeaderName(){
        var result = Utils.checkHeaderName("SIO#$#JHNDkkk2ss88980", "_");
        assertEquals("sio___jhndkkk2ss88980", result);
    }

    /**
     * Tests the positive scenario of checkHeaderName when the header starts with invalid chars
     */
    @Test public void checkHeaderNameBad(){
        var result = Utils.checkHeaderName("0034358sSD#@#Hs", "_");
        assertEquals("0034358ssd___hs", result);
    }
}
