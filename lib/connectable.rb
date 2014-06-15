require "connectable/version"

module Connectable
  extend ActiveSupport::Concern
  mattr_accessor :debug
  self.debug = false

  module ClassMethods    
    # Example:
    #  connects_to :image
    #
    # Generates
    #   belongs_to :image, :foreign_key => :to_id
    # 
    #   has_many :thing_images, :class_name => 'ThingImage', :foreign_key => :from_id
    #   has_many :images, has_many_proc, :through => :thing_images

    def connects_to(destination_type, options = {})
      return unless ActiveRecord::Base.connected? && table_exists?
      
      # How we refer to the collection
      collection_name = (options[:as] || destination_type).to_sym
      
      if connection_already_defined?(collection_name)
         raise RuntimeError, "Connection #{collection_name} already defined on #{self.name}. Was it inherited?."  if Connectable.debug
        return
      end
      
      # Just stack when defined - it'll be instantiated on demand
      unless options.delete(:now)
        connections[collection_name] = {:klass => self, :destination => destination_type, :options => options}
        return
      end
      
      # The destination class through the join
      destination_class = destination_type.to_s.classify.constantize
      return unless destination_class.table_exists?
            
      # The collection name for the join table
      join_collection = (collection_name.to_s.singularize + "_connections").to_sym

      # The join class in the case (a) collection name is the destination_type and (b) collection name is different from destination_type
      if options[:as].nil?
        join_class_name = options[:inverse_of] ? "#{destination_type.to_s.singularize}_#{self.name}" : "#{self.name}_#{destination_type.to_s.singularize}"
      elsif options[:inverse_of] && options[:inverse_of].to_s.classify.constantize < ::Thing
        join_class_name = "#{destination_type.to_s.singularize}_#{self.name}"
      else
        join_class_name = (options[:inverse_of] || options[:as]).to_s
      end
      join_class = join_class_name.classify.constantize
      
      # Create the relationships
      direction = 'symmetric' if options.delete(:symmetric)
      direction ||= options.delete(:inverse_of) ? 'inverse' : 'forward'
      options.delete(:as)
      send "connects_to_#{direction}", destination_class, collection_name, join_class, join_collection, options
    end
    alias :connects_with :connects_to
          
    def destroy_all
      instantiate_connections
      super
    end
    
    def instantiate_connections
      return if @_instantiated_connections
      ancestor.instantiate_connections if ancestor
      puts "Instantiating connections for class #{self.name}" if Connectable.debug
      connections.dup.each do |name, connection|
        next if connection[:klass].connection_already_defined?(connection[:options][:as] || connection[:destination])
        puts "Instantiating connection #{connection[:klass]}.#{connection[:options][:as] || connection[:destination]}"  if Connectable.debug
        connection[:klass].send :connects_to, connection[:destination], connection[:options].merge(:now => true)
      end
      @_instantiated_connections = true
    end
    
    def connection_already_defined?(collection_name)
      reflections.keys.include?(collection_name)
    end
    
    def connections
      @connections ||= {}
    end
    
    def symmetric_collection
      @symmetric_collection ||= {}
    end

  private
    def connects_to_forward(destination_class, collection_name, join_class, join_collection, options = {})
      join_table  = join_class.table_name
      join_class.send :belongs_to, collection_name, :class_name => "::#{destination_class.to_s}", :foreign_key => :to_id

      # Forward connection
      has_many join_collection, :class_name => "::#{join_class.to_s}", :foreign_key => :from_id, :dependent => :destroy
      has_many collection_name, has_many_proc(join_table, collection_name), has_many_through_options(join_collection, destination_class, options)

      if Connectable.debug
        puts "connects_to_forward for #{self.name}"
        puts "#{join_class}.send :belongs_to, :#{collection_name}, :class_name => '::#{destination_class.to_s}', :foreign_key => :to_id"
        puts "has_many :#{join_collection}, :class_name => '::#{join_class.to_s}', :foreign_key => :from_id, :dependent => :destroy"
        puts "has_many :#{collection_name}, has_many_proc(:#{collection_name}), #{has_many_through_options(join_collection, destination_class, options).inspect}"
        puts " "
      end
    end

    def connects_to_inverse(destination_class, collection_name, join_class, join_collection, options = {})
      join_table  = join_class.table_name
      join_class.send :belongs_to, collection_name, :class_name => "::#{destination_class.to_s}", :foreign_key => :from_id
      
      has_many join_collection, :class_name => "::#{join_class.to_s}", :foreign_key => :to_id, :dependent => :destroy
      has_many collection_name, has_many_proc(join_table, join_class), has_many_through_options(join_collection, destination_class, options)
    
      if Connectable.debug
        puts "connects_to_inverse for #{self.name}"
        puts "#{join_class}.send :belongs_to, :#{collection_name}, :class_name => '::#{destination_class.to_s}', :foreign_key => :to_id"
        puts "has_many :#{join_collection}, :class_name => '::#{join_class.to_s}', :foreign_key => :from_id, :dependent => :destroy"
        puts "has_many :#{collection_name}, has_many_proc(:#{join_table}, #{join_class}), #{has_many_through_options(join_collection, destination_class, options).inspect}"
        puts " "
      end
    end
    
    # Mark that this collection is symmetric so we'll add two records when creating a new relationship
    def connects_to_symmetric(destination_class, collection_name, join_class, join_collection, options = {})
      symmetric_collection[collection_name] = true
      connects_to_forward(destination_class, collection_name, join_class, join_collection, options = {})
    end
      
    def has_many_proc(join_table, collection_name = join_table)
      -> s {
        extending!(Connectable::AssociationMethods)
        # select("#{join_table.to_s}.tableoid::regclass as #{join_table.to_s.singularize}_#{inheritance_column}")
      }
    end

    def has_many_through_options(join_collection, destination_class, options)    
      {
        :through => join_collection, 
        :as => destination_class.to_s.underscore.downcase.pluralize.to_sym
      }.merge(options)
    end
    
    # This is the prefered strategy but we can't do a UNION in ActiveRecord::Relation and so can't return a relation
    # We can do this and then execute find_by_SQL but that returns rows, not a Relation.  So we'll fall back to a strategy
    # of inserting 2 records in the join, one each direction, for symmetric connections.
    def _connects_to_symmetric(destination_class, collection_name, join_class, join_collection, options = {})
      connects_to_forward(destination_class, "forward_#{collection_name}".to_sym, join_class, "forward_#{join_collection}".to_sym, options)
      connects_to_inverse(destination_class, "inverse_#{collection_name}".to_sym, join_class, "inverse_#{join_collection}".to_sym, options)
      # g."forward_#{collection_name}".to_sym.where(nil).union(g."inverse_#{collection_name}".where(nil))
    end

  end
  
  module AssociationMethods
    # Add record to an association and also allow setting of the attributes
    # on the join table
    def add(source, assoc_attributes = {}, do_symmetric = true)
      target_class_name = proxy_association.owner.class.name
      source_class_name = proxy_association.source_reflection.class_name
      
      association_name = proxy_association.reflection.name
      connection_name  = proxy_association.reflection.options[:through]      

      # raise ArgumentError, "#{target_class_name}.#{association_name}.add: #{source_class_name} object must be provided" unless source.present? && source.class.name == source_class_name
      connection = proxy_association.owner.send(connection_name).build(assoc_attributes)
      connection.send "#{association_name}=", source
      
      connection.save!
      if symmetric_collection[association_name] && do_symmetric
        source.send(association_name).add proxy_association.owner, assoc_attributes, false
      end
      connection
    end
  end
  
  def destroy
    self.class.instantiate_connections
    super
  end
  
  # Create the connection and invoke it
  def method_missing(method, *args, &block)
    connection_name = $1.pluralize.to_sym if method.to_s =~ /(.+?)(_ids)?(=)?$/
    if connection = connection_definition(connection_name || method)
      puts "#{self.class.name} is building connection for #{connection[:klass].name}##{connection_name || method} in method_missing " if Connectable.debug
      connection[:klass].send :connects_to, connection[:destination], connection[:options].merge(:now => true)
      send method, *args
    else
      super
    end
  end
  
private
  def connection_definition(name)
    connection = nil
    [self.class, *self.class.ar_ancestors].each do |klass|
      break if connection = klass.connections[name]
    end
    connection
  end
end

