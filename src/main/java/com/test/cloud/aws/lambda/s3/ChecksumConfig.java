package com.test.cloud.aws.lambda.s3;

/**
 * Provides access to the configurable {@link Checksum} properties.
 *
 * <p>
 * {@link #bufferSize()}: The size of the buffer used during the download/hashing
 * operation. Can be customized with the environment variable {@code BUFFER_SIZE}.
 * Defaults to 10MB.
 *
 * <p>
 * {@link #partSize()}: Determines the maximum amount of data hashed per function
 * execution. If the file size is greater than this value, the lambda will re-trigger
 * itself to continue the operation in another request. Can be customized with the
 * environment variable {@code PART_SIZE}. Defaults to 5GB.
 *
 * <p>
 * {@link #algorithm()}: The digest algorithm used by the function. Supported algorithms
 * are {@code SHA256} and {@code MD5}. Can be customized with the environment variable
 * {@code DIGEST_ALGORITHM}. Defaults to {@code SHA256}.
 *
 * <p>
 * {@link #metaKeyHash()}: The S3 object metadata key used to hold the hash produced by
 * the function. This value is automatically prefixed with "x-amz-meta-". Can be
 * customized with the environment variable {@code METADATA_KEY_HASH}. Defaults to
 * {@code sha256}.
 *
 * <p>
 * {@link #metaKeyPartialHash()}: The S3 object metadata key used to hold the partial hash
 * calculation value when the file is being hashed through multiple function calls. Can be
 * customized with the environment variable {@code METADATA_KEY_PARTIAL_HASH}. Defaults to
 * {@code partial-hash}.
 *
 * <p>
 * {@link #metaKeyHashProgress()}: The S3 object metadata key used to hold the 0-based
 * index position of the last byte processed when the file is being hashed through
 * multiple function calls. Can be customized with the environment variable
 * {@code METADATA_KEY_PARTS_HASHED}. Defaults to {@code hash-progress}.
 */
public class ChecksumConfig
{
    // environment variables

    private static final String ENV_BUFFER_SIZE = "BUFFER_SIZE";

    private static final String ENV_PART_SIZE = "PART_SIZE";

    private static final String ENV_DIGEST_ALGORITHM = "DIGEST_ALGORITHM";

    private static final String ENV_META_HASH = "METADATA_KEY_HASH";

    private static final String ENV_META_PARTIAL_HASH = "METADATA_KEY_PARTIAL_HASH";

    private static final String ENV_META_HASH_PROGRESS = "METADATA_KEY_PARTS_HASHED";

    // default configuration values

    private static final int DEFAULT_BUFFER_SIZE = 10 * 1024 * 1024;

    private static final long DEFAULT_PART_SIZE = 5L * 1024 * 1024 * 1024;

    private static final String DEFAULT_DIGEST_ALGORITHM = "SHA256";

    private static final String DEFAULT_META_PARTIAL_HASH = "partial-hash";

    private static final String DEFAULT_META_HASH_PROGRESS = "hash-progress";

    // configuration values

    private final int bufferSize;

    private final long partSize;

    private final ChecksumAlgorithm algorithm;

    private final String metaKeyHash;

    private final String metaKeyPartialHash;

    private final String metaKeyHashProgress;

    ChecksumConfig()
    {
        this.bufferSize = getEnv(ENV_BUFFER_SIZE, DEFAULT_BUFFER_SIZE);

        this.partSize = getEnv(ENV_PART_SIZE, DEFAULT_PART_SIZE);

        this.algorithm = ChecksumAlgorithm.valueOf(getEnv(ENV_DIGEST_ALGORITHM, DEFAULT_DIGEST_ALGORITHM));

        this.metaKeyHash = getEnv(ENV_META_HASH, this.algorithm.name().toLowerCase());

        this.metaKeyPartialHash = getEnv(ENV_META_PARTIAL_HASH, DEFAULT_META_PARTIAL_HASH);

        this.metaKeyHashProgress = getEnv(ENV_META_HASH_PROGRESS, DEFAULT_META_HASH_PROGRESS);
    }

    public int bufferSize()
    {
        return bufferSize;
    }

    public long partSize()
    {
        return partSize;
    }

    public ChecksumAlgorithm algorithm()
    {
        return algorithm;
    }

    public String metaKeyHash()
    {
        return metaKeyHash;
    }

    public String metaKeyPartialHash()
    {
        return metaKeyPartialHash;
    }

    public String metaKeyHashProgress()
    {
        return metaKeyHashProgress;
    }

    private String getEnv(String name, String defaultValue)
    {
        String value = System.getenv(name);
        return ((value == null) || (value = value.trim()).isEmpty()) ? defaultValue : value;
    }

    private int getEnv(String name, int defaultValue)
    {
        String strValue = getEnv(name, "");
        if (strValue.isEmpty())
        {
            return defaultValue;
        }
        return Integer.parseInt(strValue);
    }

    private long getEnv(String name, long defaultValue)
    {
        String strValue = getEnv(name, "");
        if (strValue.isEmpty())
        {
            return defaultValue;
        }
        return Long.parseLong(strValue);
    }

}
