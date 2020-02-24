package com.test.cloud.aws.lambda.s3;

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
