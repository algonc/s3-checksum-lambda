package com.test.cloud.aws.lambda.s3;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.function.Function;
import java.util.function.Supplier;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.bouncycastle.crypto.digests.EncodableDigest;
import org.bouncycastle.crypto.digests.GeneralDigest;
import org.bouncycastle.crypto.digests.MD5Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;

/**
 * Computes the checksum of an S3 object, saving the result as object's metadata.
 */
public class Checksum implements RequestHandler<S3Event, String>
{
    /**
     * S3 client
     */
    private final AmazonS3 s3;

    /**
     * Lambda client
     */
    private final AWSLambda lambda;

    /**
     * Function configuration
     */
    private final ChecksumConfig config;

    /**
     * The supplier of the {@link GeneralDigest} which serves the configured algorithm.
     */
    private final Supplier<GeneralDigest> digestSupplier;

    /**
     * The supplier of the resuming {@link GeneralDigest} which serves the configured
     * algorithm.
     */
    private final Function<byte[], GeneralDigest> resumingDigestSupplier;

    public Checksum()
    {
        this(AmazonS3ClientBuilder.standard().build(),
             AWSLambdaClientBuilder.defaultClient(),
             new ChecksumConfig());
    }

    Checksum(AmazonS3 s3, AWSLambda lambda, ChecksumConfig config)
    {
        this.s3 = s3;
        this.lambda = lambda;
        this.config = config;
        switch (config.algorithm())
        {
            case SHA256:
                digestSupplier = SHA256Digest::new;
                resumingDigestSupplier = SHA256Digest::new;
                break;
            case MD5:
                digestSupplier = MD5Digest::new;
                resumingDigestSupplier = MD5Digest::new;
                break;
            default:
                throw new IllegalStateException("Not supported: " + config.algorithm());
        }
    }

    @Override
    public String handleRequest(S3Event event, Context context)
    {
        LambdaLogger logger = context.getLogger();

        S3EventNotificationRecord s3event = event.getRecords().get(0);

        logger.log("Received event: " + s3event.getEventName());

        String bucket = s3event.getS3().getBucket().getName();
        String key = s3event.getS3().getObject().getKey();

        logger.log("Computing hash of object in bucket '" + bucket + "' with key '" + key + "'");

        ObjectMetadata metadata = s3.getObjectMetadata(bucket, key);
        long fileSize = metadata.getContentLength();

        if (fileSize < 1)
        {
            logger.log("Ignoring object in bucket '" + bucket + "' with key '" + key + "': Unexpected Content-Length: " + fileSize);
            return "";
        }

        GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, key);

        boolean isPartialHash = fileSize > config.partSize();
        byte[] previousState = null;
        long end = fileSize - 1;

        if (isPartialHash)
        {
            previousState = previousHashState(metadata);

            long previousEnd = lastHashedByte(metadata);

            long start = previousEnd == 0 ? 0 : previousEnd + 1;

            end = (start + config.partSize()) - 1;

            if (end >= fileSize)
            {
                end = fileSize - 1;
            }

            logger.log("Hash of object will be computed on a range of bytes [" + start + "-" + end + "]");

            getObjectRequest.setRange(start, end);
        }

        boolean finalizeHash = end == (fileSize - 1);

        GeneralDigest hasher = hasher(previousState);

        downloadAndCompute(getObjectRequest, hasher, logger);

        String hash = "";
        boolean retrigger = false;

        if (finalizeHash)
        {
            hash = generateHash(logger, bucket, key, hasher, metadata);
            metadata.getUserMetadata().remove(config.metaKeyPartialHash());
            metadata.getUserMetadata().remove(config.metaKeyHashProgress());
            metadata.addUserMetadata(config.metaKeyHash(), hash);
        }
        else
        {
            metadata.addUserMetadata(config.metaKeyPartialHash(), hasherState(hasher));
            metadata.addUserMetadata(config.metaKeyHashProgress(), String.valueOf(end));
            retrigger = true;
        }

        updateMetadata(bucket, key, metadata);

        logger.log("Object metadata updated successfully");

        if (retrigger)
        {
            retriggerEvent(event, context);
        }

        return hash;
    }

    private void retriggerEvent(S3Event event, Context context)
    {
        InvokeRequest invokeRequest = new InvokeRequest().withFunctionName(context.getFunctionName())
                                                         .withInvocationType(InvocationType.Event)
                                                         .withPayload(event.toJson());

        try
        {
            InvokeResult invoke = lambda.invoke(invokeRequest);

            if (invoke.getStatusCode() == 202)
            {
                context.getLogger().log("Hash computation in progress. Lambda re-triggered successfully");
            }
            else
            {
                context.getLogger().log("ERROR: Failed to re-trigger lambda. Status code: " + invoke.getStatusCode() + ", Type: " + invoke.getFunctionError());
            }
        }
        catch (Exception e)
        {
            context.getLogger().log("ERROR: Failed to re-trigger lambda: " + e);
        }
    }

    private long downloadAndCompute(GetObjectRequest getObjectRequest, GeneralDigest hasher, LambdaLogger logger)
    {
        try (S3Object s3Object = s3.getObject(getObjectRequest))
        {
            ObjectMetadata metadata = s3Object.getObjectMetadata();

            long bytesToHash = metadata.getContentLength();

            compute(hasher, s3Object, bytesToHash);

            logger.log("Computed hash of " + bytesToHash + " bytes");

            return bytesToHash;
        }
        catch (IOException ex)
        {
            logger.log("ERROR: Computing hash: " + ex.getMessage());
            throw new UncheckedIOException(ex);
        }
    }

    private String generateHash(LambdaLogger logger, String bucket, String key, GeneralDigest hasher, ObjectMetadata metadata)
    {
        byte[] hashOut = new byte[hasher.getDigestSize()];
        hasher.doFinal(hashOut, 0);

        String hash = String.valueOf(Hex.encodeHex(hashOut));

        String logMsg = "Computed hash of object in bucket '" + bucket + "' with key '" + key + "'\n"
                + "Content-Type: " + metadata.getContentType() + "\n"
                + "Content-Length: " + metadata.getContentLength() + "\n"
                + config.algorithm() + " Hash: " + hash;
        logger.log(logMsg);

        return hash;
    }

    private void updateMetadata(String bucket, String key, ObjectMetadata metadata)
    {
        AccessControlList acl = s3.getObjectAcl(bucket, key);

        CopyObjectRequest request = new CopyObjectRequest(bucket, key, bucket, key).withNewObjectMetadata(metadata)
                                                                                   .withAccessControlList(acl);

        s3.copyObject(request);
    }

    private void compute(GeneralDigest hasher, S3Object s3Object, long contentLength) throws IOException
    {
        byte[] buffer = new byte[config.bufferSize()];

        try (InputStream stream = s3Object.getObjectContent())
        {
            int nRead;
            while ((nRead = stream.read(buffer, 0, buffer.length)) > -1)
            {
                hasher.update(buffer, 0, nRead);
                contentLength -= nRead;
            }
        }

        if (contentLength > 0)
        {
            throw new IOException("Could not read all data - missing bytes: " + contentLength);
        }
    }

    private long lastHashedByte(ObjectMetadata metadata)
    {
        String partNumStr = metadata.getUserMetaDataOf(config.metaKeyHashProgress());

        if ((partNumStr == null) || partNumStr.isEmpty())
        {
            return 0;
        }
        else
        {
            return Integer.parseInt(partNumStr);
        }
    }

    private byte[] previousHashState(ObjectMetadata metadata)
    {
        String previousStateHex = metadata.getUserMetaDataOf(config.metaKeyPartialHash());

        if ((previousStateHex == null) || previousStateHex.isEmpty())
        {
            return null;
        }

        try
        {
            return Hex.decodeHex(previousStateHex.toCharArray());
        }
        catch (DecoderException e)
        {
            throw new IllegalStateException("The value of the '" + config.metaKeyPartialHash() + "' metadata (" + previousStateHex + ") cannot be decoded as hexadecimal value: " + e, e);
        }
    }

    private GeneralDigest hasher(byte[] previousState)
    {
        if (previousState == null)
        {
            return digestSupplier.get();
        }
        return resumingDigestSupplier.apply(previousState);
    }

    private String hasherState(GeneralDigest hasher)
    {
        byte[] encodedState = ((EncodableDigest) hasher).getEncodedState();
        return Hex.encodeHexString(encodedState);
    }

}
