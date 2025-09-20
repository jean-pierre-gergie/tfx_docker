
# preprocessing.py (fixed)
# preprocessing.py (cast integer outputs to int64)
# preprocessing.py (robust scalarization for varlen features)
# preprocessing.py (final, reduced + robust)
import tensorflow as tf
import tensorflow_transform as tft

# ======================
# Columns to keep
# ======================
LABEL_KEY = 'isFraud'
NUMERIC_FLOAT_KEYS = [
    'amount',
    'oldbalanceOrg',
    'newbalanceOrig',
    'oldbalanceDest',
    'newbalanceDest',
]
_XF = '_xf'
ZSCORE_CLIP = 5.0


# ======================
# Robust scalarization
# ======================
def _first_with_default_ragged(rt: tf.RaggedTensor, default_value, out_dtype):
    """First element per row; empty-row -> default; handles global-empty safely."""
    batch_size = tf.shape(rt.row_splits)[0] - 1  # N rows
    total_vals = tf.shape(rt.flat_values)[0]

    def all_empty_case():
        return tf.fill([batch_size], tf.cast(default_value, out_dtype))

    def normal_case():
        splits = rt.row_splits  # [N+1]
        starts = splits[:-1]
        lens = splits[1:] - splits[:-1]
        has = tf.not_equal(lens, 0)
        safe_idx = tf.where(has, starts, tf.zeros_like(starts))
        gathered = tf.gather(rt.flat_values, safe_idx)  # safe (flat_values > 0)
        gathered = tf.cast(gathered, out_dtype)
        default_tensor = tf.cast(default_value, out_dtype)
        return tf.where(has, gathered, tf.fill([batch_size], default_tensor))

    return tf.cond(total_vals > 0, normal_case, all_empty_case)


def _scalar_from_any(x, default_value, out_dtype):
    """Convert Sparse/Ragged/Dense (possibly varlen) to shape [batch] scalar tensor."""
    # Sparse -> Ragged
    if isinstance(x, tf.SparseTensor):
        return _first_with_default_ragged(tf.RaggedTensor.from_sparse(x), default_value, out_dtype)

    # Ragged
    if isinstance(x, tf.RaggedTensor):
        return _first_with_default_ragged(x, default_value, out_dtype)

    # Dense
    x = tf.convert_to_tensor(x)
    # If rank==1, already [batch]
    if x.shape.rank == 1:
        return tf.cast(x, out_dtype)

    # General dense fallback: choose first column if exists, otherwise defaults
    batch = tf.shape(x)[0]
    width = tf.shape(x)[-1]

    def empty_case():
        return tf.fill([batch], tf.cast(default_value, out_dtype))

    def nonempty_case():
        # If it's [batch, 1] this also works
        return tf.cast(x[:, 0], out_dtype)

    return tf.cond(width > 0, nonempty_case, empty_case)


def _to_float_scalar(x, default=0.0):
    t = _scalar_from_any(x, default_value=default, out_dtype=tf.float32)
    # If string, coerce to number
    if t.dtype == tf.string:
        s = tf.where(tf.equal(t, b''), tf.constant(str(default).encode()), t)
        t = tf.strings.to_number(s, out_type=tf.float32)
    else:
        t = tf.cast(t, tf.float32)
    return t


def _to_int64_scalar(x, default=0):
    t = _scalar_from_any(x, default_value=default, out_dtype=tf.int64)
    if t.dtype == tf.string:
        s = tf.where(tf.equal(t, b''), tf.constant(str(default).encode()), t)
        f = tf.strings.to_number(s, out_type=tf.float32)
        t = tf.cast(tf.round(f), tf.int64)
    elif t.dtype.is_floating:
        t = tf.cast(tf.round(t), tf.int64)
    else:
        t = tf.cast(t, tf.int64)
    return t


def _clip_z(x, clip=ZSCORE_CLIP):
    return tf.clip_by_value(x, -clip, clip)


# ======================
# Main preprocessing
# ======================
def preprocessing_fn(inputs):
    outputs = {}

    # Label -> int64 {0,1}
    if LABEL_KEY in inputs:
        label = _to_int64_scalar(inputs[LABEL_KEY], default=0)
        label = tf.clip_by_value(label, 0, 1)
        outputs[LABEL_KEY + _XF] = tf.cast(label, tf.int64)

    # Selected numeric features -> float32 z-score (clipped)
    for k in NUMERIC_FLOAT_KEYS:
        if k in inputs:
            v = _to_float_scalar(inputs[k], default=0.0)
            v = tft.scale_to_z_score(v)
            v = _clip_z(v)
            outputs[k + _XF] = v

    return outputs

