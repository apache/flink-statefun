// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: commands.proto

package org.apache.flink.statefun.e2e.smoke.generated;

/**
 * Protobuf type {@code org.apache.flink.statefun.e2e.smoke.SourceCommand}
 */
public  final class SourceCommand extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:org.apache.flink.statefun.e2e.smoke.SourceCommand)
    SourceCommandOrBuilder {
private static final long serialVersionUID = 0L;
  // Use SourceCommand.newBuilder() to construct.
  private SourceCommand(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private SourceCommand() {
  }

  @Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private SourceCommand(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    this();
    if (extensionRegistry == null) {
      throw new NullPointerException();
    }
    int mutable_bitField0_ = 0;
    com.google.protobuf.UnknownFieldSet.Builder unknownFields =
        com.google.protobuf.UnknownFieldSet.newBuilder();
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          case 8: {

            target_ = input.readInt32();
            break;
          }
          case 18: {
            Commands.Builder subBuilder = null;
            if (commands_ != null) {
              subBuilder = commands_.toBuilder();
            }
            commands_ = input.readMessage(Commands.parser(), extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(commands_);
              commands_ = subBuilder.buildPartial();
            }

            break;
          }
          default: {
            if (!parseUnknownField(
                input, unknownFields, extensionRegistry, tag)) {
              done = true;
            }
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return CommandsOuterClass.internal_static_org_apache_flink_statefun_e2e_smoke_SourceCommand_descriptor;
  }

  @Override
  protected FieldAccessorTable
      internalGetFieldAccessorTable() {
    return CommandsOuterClass.internal_static_org_apache_flink_statefun_e2e_smoke_SourceCommand_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            SourceCommand.class, Builder.class);
  }

  public static final int TARGET_FIELD_NUMBER = 1;
  private int target_;
  /**
   * <code>int32 target = 1;</code>
   */
  public int getTarget() {
    return target_;
  }

  public static final int COMMANDS_FIELD_NUMBER = 2;
  private Commands commands_;
  /**
   * <code>.org.apache.flink.statefun.e2e.smoke.Commands commands = 2;</code>
   */
  public boolean hasCommands() {
    return commands_ != null;
  }
  /**
   * <code>.org.apache.flink.statefun.e2e.smoke.Commands commands = 2;</code>
   */
  public Commands getCommands() {
    return commands_ == null ? Commands.getDefaultInstance() : commands_;
  }
  /**
   * <code>.org.apache.flink.statefun.e2e.smoke.Commands commands = 2;</code>
   */
  public CommandsOrBuilder getCommandsOrBuilder() {
    return getCommands();
  }

  private byte memoizedIsInitialized = -1;
  @Override
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  @Override
  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (target_ != 0) {
      output.writeInt32(1, target_);
    }
    if (commands_ != null) {
      output.writeMessage(2, getCommands());
    }
    unknownFields.writeTo(output);
  }

  @Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (target_ != 0) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt32Size(1, target_);
    }
    if (commands_ != null) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(2, getCommands());
    }
    size += unknownFields.getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof SourceCommand)) {
      return super.equals(obj);
    }
    SourceCommand other = (SourceCommand) obj;

    if (getTarget()
        != other.getTarget()) return false;
    if (hasCommands() != other.hasCommands()) return false;
    if (hasCommands()) {
      if (!getCommands()
          .equals(other.getCommands())) return false;
    }
    if (!unknownFields.equals(other.unknownFields)) return false;
    return true;
  }

  @Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    hash = (37 * hash) + TARGET_FIELD_NUMBER;
    hash = (53 * hash) + getTarget();
    if (hasCommands()) {
      hash = (37 * hash) + COMMANDS_FIELD_NUMBER;
      hash = (53 * hash) + getCommands().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static SourceCommand parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static SourceCommand parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static SourceCommand parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static SourceCommand parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static SourceCommand parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static SourceCommand parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static SourceCommand parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static SourceCommand parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static SourceCommand parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static SourceCommand parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static SourceCommand parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static SourceCommand parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  @Override
  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(SourceCommand prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  @Override
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @Override
  protected Builder newBuilderForType(
      BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code org.apache.flink.statefun.e2e.smoke.SourceCommand}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:org.apache.flink.statefun.e2e.smoke.SourceCommand)
      SourceCommandOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return CommandsOuterClass.internal_static_org_apache_flink_statefun_e2e_smoke_SourceCommand_descriptor;
    }

    @Override
    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return CommandsOuterClass.internal_static_org_apache_flink_statefun_e2e_smoke_SourceCommand_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              SourceCommand.class, Builder.class);
    }

    // Construct using org.apache.flink.statefun.e2e.smoke.generated.SourceCommand.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
      }
    }
    @Override
    public Builder clear() {
      super.clear();
      target_ = 0;

      if (commandsBuilder_ == null) {
        commands_ = null;
      } else {
        commands_ = null;
        commandsBuilder_ = null;
      }
      return this;
    }

    @Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return CommandsOuterClass.internal_static_org_apache_flink_statefun_e2e_smoke_SourceCommand_descriptor;
    }

    @Override
    public SourceCommand getDefaultInstanceForType() {
      return SourceCommand.getDefaultInstance();
    }

    @Override
    public SourceCommand build() {
      SourceCommand result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @Override
    public SourceCommand buildPartial() {
      SourceCommand result = new SourceCommand(this);
      result.target_ = target_;
      if (commandsBuilder_ == null) {
        result.commands_ = commands_;
      } else {
        result.commands_ = commandsBuilder_.build();
      }
      onBuilt();
      return result;
    }

    @Override
    public Builder clone() {
      return super.clone();
    }
    @Override
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        Object value) {
      return super.setField(field, value);
    }
    @Override
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return super.clearField(field);
    }
    @Override
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return super.clearOneof(oneof);
    }
    @Override
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, Object value) {
      return super.setRepeatedField(field, index, value);
    }
    @Override
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        Object value) {
      return super.addRepeatedField(field, value);
    }
    @Override
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof SourceCommand) {
        return mergeFrom((SourceCommand)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(SourceCommand other) {
      if (other == SourceCommand.getDefaultInstance()) return this;
      if (other.getTarget() != 0) {
        setTarget(other.getTarget());
      }
      if (other.hasCommands()) {
        mergeCommands(other.getCommands());
      }
      this.mergeUnknownFields(other.unknownFields);
      onChanged();
      return this;
    }

    @Override
    public final boolean isInitialized() {
      return true;
    }

    @Override
    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      SourceCommand parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (SourceCommand) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private int target_ ;
    /**
     * <code>int32 target = 1;</code>
     */
    public int getTarget() {
      return target_;
    }
    /**
     * <code>int32 target = 1;</code>
     */
    public Builder setTarget(int value) {
      
      target_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>int32 target = 1;</code>
     */
    public Builder clearTarget() {
      
      target_ = 0;
      onChanged();
      return this;
    }

    private Commands commands_;
    private com.google.protobuf.SingleFieldBuilderV3<
        Commands, Commands.Builder, CommandsOrBuilder> commandsBuilder_;
    /**
     * <code>.org.apache.flink.statefun.e2e.smoke.Commands commands = 2;</code>
     */
    public boolean hasCommands() {
      return commandsBuilder_ != null || commands_ != null;
    }
    /**
     * <code>.org.apache.flink.statefun.e2e.smoke.Commands commands = 2;</code>
     */
    public Commands getCommands() {
      if (commandsBuilder_ == null) {
        return commands_ == null ? Commands.getDefaultInstance() : commands_;
      } else {
        return commandsBuilder_.getMessage();
      }
    }
    /**
     * <code>.org.apache.flink.statefun.e2e.smoke.Commands commands = 2;</code>
     */
    public Builder setCommands(Commands value) {
      if (commandsBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        commands_ = value;
        onChanged();
      } else {
        commandsBuilder_.setMessage(value);
      }

      return this;
    }
    /**
     * <code>.org.apache.flink.statefun.e2e.smoke.Commands commands = 2;</code>
     */
    public Builder setCommands(
        Commands.Builder builderForValue) {
      if (commandsBuilder_ == null) {
        commands_ = builderForValue.build();
        onChanged();
      } else {
        commandsBuilder_.setMessage(builderForValue.build());
      }

      return this;
    }
    /**
     * <code>.org.apache.flink.statefun.e2e.smoke.Commands commands = 2;</code>
     */
    public Builder mergeCommands(Commands value) {
      if (commandsBuilder_ == null) {
        if (commands_ != null) {
          commands_ =
            Commands.newBuilder(commands_).mergeFrom(value).buildPartial();
        } else {
          commands_ = value;
        }
        onChanged();
      } else {
        commandsBuilder_.mergeFrom(value);
      }

      return this;
    }
    /**
     * <code>.org.apache.flink.statefun.e2e.smoke.Commands commands = 2;</code>
     */
    public Builder clearCommands() {
      if (commandsBuilder_ == null) {
        commands_ = null;
        onChanged();
      } else {
        commands_ = null;
        commandsBuilder_ = null;
      }

      return this;
    }
    /**
     * <code>.org.apache.flink.statefun.e2e.smoke.Commands commands = 2;</code>
     */
    public Commands.Builder getCommandsBuilder() {
      
      onChanged();
      return getCommandsFieldBuilder().getBuilder();
    }
    /**
     * <code>.org.apache.flink.statefun.e2e.smoke.Commands commands = 2;</code>
     */
    public CommandsOrBuilder getCommandsOrBuilder() {
      if (commandsBuilder_ != null) {
        return commandsBuilder_.getMessageOrBuilder();
      } else {
        return commands_ == null ?
            Commands.getDefaultInstance() : commands_;
      }
    }
    /**
     * <code>.org.apache.flink.statefun.e2e.smoke.Commands commands = 2;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        Commands, Commands.Builder, CommandsOrBuilder>
        getCommandsFieldBuilder() {
      if (commandsBuilder_ == null) {
        commandsBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            Commands, Commands.Builder, CommandsOrBuilder>(
                getCommands(),
                getParentForChildren(),
                isClean());
        commands_ = null;
      }
      return commandsBuilder_;
    }
    @Override
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    @Override
    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:org.apache.flink.statefun.e2e.smoke.SourceCommand)
  }

  // @@protoc_insertion_point(class_scope:org.apache.flink.statefun.e2e.smoke.SourceCommand)
  private static final SourceCommand DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new SourceCommand();
  }

  public static SourceCommand getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<SourceCommand>
      PARSER = new com.google.protobuf.AbstractParser<SourceCommand>() {
    @Override
    public SourceCommand parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new SourceCommand(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<SourceCommand> parser() {
    return PARSER;
  }

  @Override
  public com.google.protobuf.Parser<SourceCommand> getParserForType() {
    return PARSER;
  }

  @Override
  public SourceCommand getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

