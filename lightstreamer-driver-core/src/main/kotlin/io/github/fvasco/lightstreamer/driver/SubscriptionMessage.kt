package io.github.fvasco.lightstreamer.driver

import java.util.*

/**
 * Subscription event
 */
sealed class SubscriptionMessage {

    object ClearSnapshot : SubscriptionMessage()

    class Update(val subscription: Subscription, override val values: List<String?>) : SubscriptionMessage(), Map<String, String?> {
        override val entries: Set<Map.Entry<String, String?>> by lazy {
            object : Set<Map.Entry<String, String?>> {
                override val size: Int
                    get() = values.size

                override fun contains(element: Map.Entry<String, String?>): Boolean =
                        this@Update.contains(element.key) && this@Update[element.key] == element.value

                override fun containsAll(elements: Collection<Map.Entry<String, String?>>): Boolean =
                        elements.all { contains(it) }

                override fun isEmpty(): Boolean = false

                override fun iterator(): Iterator<Map.Entry<String, String?>> =
                        subscription.itemNames.asSequence().mapIndexed { i, s -> MapEntry(s, values[i]) }.iterator()
            }
        }
        override val keys: Set<String> by lazy {
            val fields = subscription.itemNames
            object : Set<String> {
                override val size: Int get() = fields.size

                override fun contains(element: String): Boolean = this@Update.containsKey(element)

                override fun containsAll(elements: Collection<String>): Boolean = elements.all { contains(it) }

                override fun isEmpty(): Boolean = false

                override fun iterator(): Iterator<String> = fields.iterator()
            }
        }

        override val size: Int
            get() = values.size

        override fun containsKey(key: String): Boolean =
                Collections.binarySearch(subscription.itemNames, key, Subscription.itemNameComparator) >= 0

        override fun containsValue(value: String?): Boolean = values.contains(value)

        override fun isEmpty(): Boolean = false

        init {
            assert(values.size == subscription.itemNames.size)
        }


        override operator fun get(key: String): String? {
            val itemPos = Collections.binarySearch(subscription.itemNames, key, Subscription.itemNameComparator)
            return if (itemPos == -1) null else values[itemPos]
        }

        override fun toString(): String = entries.joinToString(", ", "{", "}")
    }

    object SubscriptionOk : SubscriptionMessage()

    private class MapEntry(override val key: String, override val value: String?) : Map.Entry<String, String?> {
        override fun toString(): String = "$key=$value"
    }
}
