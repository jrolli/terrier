#pragma once
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/constants.h"
#include "common/macros.h"
#include "common/strong_typedef.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"
#include "type/type_util.h"

namespace terrier::catalog {

/**
 * Internal object for representing SQL table schema. Currently minimal until we add more features to the system.
 * TODO(Matt): we should make sure to revisit the fields and their uses as we bring in a catalog to replace some of the
 * reliance on these classes
 */
class Schema {
 public:
  /**
   * Internal object for representing SQL table column. Currently minimal until we add more features to the system.
   * TODO(Matt): we should make sure to revisit the fields and their uses as we bring in a catalog to replace some of
   * the reliance on these classes
   */
  class Column {
   public:
    /**
     * Instantiates a Column object, primary to be used for building a Schema object (non VARLEN attributes)
     * @param name column name
     * @param type SQL type for this column
     * @param nullable true if the column is nullable, false otherwise
     * @param oid internal unique identifier for this column
     * @param default_value default value for this column. Null by default
     */
    Column(std::string name, const type::TypeId type, const bool nullable, const col_oid_t oid,
           byte *default_value = nullptr)
        : name_(std::move(name)),
          type_(type),
          attr_size_(type::TypeUtil::GetTypeSize(type_)),
          nullable_(nullable),
          oid_(oid),
          default_(nullptr) {
      TERRIER_ASSERT(attr_size_ == 1 || attr_size_ == 2 || attr_size_ == 4 || attr_size_ == 8,
                     "This constructor is meant for non-VARLEN columns.");
      TERRIER_ASSERT(type_ != type::TypeId::INVALID, "Attribute type cannot be INVALID.");

      // ASSUMPTION: The default_value passed in is of size attr_size_
      // Copy the passed in default value (if exists)
      if (default_value != nullptr) {
        default_ = new byte[attr_size_];
        std::memcpy(default_, default_value, attr_size_);
      }
    }

    /**
     * Instantiates a Column object, primary to be used for building a Schema object (VARLEN attributes only)
     * @param name column name
     * @param type SQL type for this column
     * @param max_varlen_size the maximum length of the varlen entry
     * @param nullable true if the column is nullable, false otherwise
     * @param oid internal unique identifier for this column
     */
    Column(std::string name, const type::TypeId type, const uint16_t max_varlen_size, const bool nullable,
           const col_oid_t oid)
        : name_(std::move(name)),
          type_(type),
          attr_size_(type::TypeUtil::GetTypeSize(type_)),
          max_varlen_size_(max_varlen_size),
          nullable_(nullable),
          oid_(oid) {
      // TODO(Sai): How to handle default values for VARLEN?
      TERRIER_ASSERT(attr_size_ == VARLEN_COLUMN, "This constructor is meant for VARLEN columns.");
      TERRIER_ASSERT(type_ != type::TypeId::INVALID, "Attribute type cannot be INVALID.");
    }

    /**
     * Free the memory allocated to default_ in the destructor
     */
    ~Column() = default;

    /**
     * @return column name
     */
    const std::string &GetName() const { return name_; }
    /**
     * @return true if the column is nullable, false otherwise
     */
    bool GetNullable() const { return nullable_; }
    /**
     * @return size of the attribute in bytes. Varlen attributes have the sign bit set.
     */
    uint8_t GetAttrSize() const { return attr_size_; }

    /**
     * @return The maximum length of this column (only valid if it's VARLEN)
     */
    uint16_t GetMaxVarlenSize() const {
      TERRIER_ASSERT(attr_size_ == VARLEN_COLUMN, "This attribute has no meaning for non-VARLEN columns.");
      return max_varlen_size_;
    }

    /**
     * @return SQL type for this column
     */
    type::TypeId GetType() const { return type_; }
    /**
     * @return internal unique identifier for this column
     */
    col_oid_t GetOid() const { return oid_; }
    /**
     * @return default value for this column
     */
    byte *GetDefault() const { return default_; }

   private:
    const std::string name_;
    const type::TypeId type_;
    uint8_t attr_size_;
    uint16_t max_varlen_size_;
    const bool nullable_;
    const col_oid_t oid_;
    byte *default_;
  };

  /**
   * Instantiates a Schema object from a vector of previously-defined Columns
   * @param columns description of this SQL table's schema as a collection of Columns
   * @param version the schema version number
   */
  explicit Schema(std::vector<Column> columns, storage::layout_version_t version = storage::layout_version_t(0))
      : version_(version), columns_(std::move(columns)) {
    TERRIER_ASSERT(!columns_.empty() && columns_.size() <= common::Constants::MAX_COL,
                   "Number of columns must be between 1 and MAX_COL.");
    for (uint32_t i = 0; i < columns_.size(); i++) {
      col_oid_to_offset[columns_[i].GetOid()] = i;
    }
  }
  /**
   * @param col_offset offset into the schema specifying which Column to access
   * @return description of the schema for a specific column
   */
  Column GetColumn(const uint32_t col_offset) const {
    TERRIER_ASSERT(col_offset < columns_.size(), "column id is out of bounds for this Schema");
    return columns_[col_offset];
  }
  /**
   * @param col_oid identifier of a Column in the schema
   * @return description of the schema for a specific column
   */
  Column GetColumn(const col_oid_t col_oid) const {
    TERRIER_ASSERT(col_oid_to_offset.count(col_oid) > 0, "col_oid does not exist in this Schema");
    const uint32_t col_offset = col_oid_to_offset.at(col_oid);
    return columns_[col_offset];
  }
  /**
   * @return description of this SQL table's schema as a collection of Columns
   */
  const std::vector<Column> &GetColumns() const { return columns_; }
  /**
   * @return version number for this schema
   */
  const storage::layout_version_t GetVersion() const { return version_; }

 private:
  const storage::layout_version_t version_;
  const std::vector<Column> columns_;
  std::unordered_map<col_oid_t, uint32_t> col_oid_to_offset;
};
}  // namespace terrier::catalog
