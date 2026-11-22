/*
 * sql_saga 1.13 update: data repairs that need the 1.13 definitions. None: an era whose checks or synchronization
 * trigger were dropped before 1.13 is reported by the final check_integrity() gate, and its repair (drop_era and add_era)
 * is the site's decision, as are any rows written while the era was unenforced.
 */
